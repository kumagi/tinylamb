/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/foreign_key.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/spill/spill_store.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/ooc/external_sorter.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/row_block_codec.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr size_t kDefaultForeignKeyBudgetBytes = 8ULL << 20;
constexpr size_t kSortedLeafTargetBytes = 16ULL << 10;
constexpr size_t kMaxTopLevelMetas = 2048;
constexpr size_t kRowPosFlushBatch = 1024;
constexpr size_t kUpdateBlockTargetBytes = 16ULL << 10;
constexpr uint64_t kRowPosNullPackSentinel =
    std::numeric_limits<uint64_t>::max();

std::atomic<size_t>& BudgetOverrideForTest() noexcept {
  static std::atomic<size_t> val{0};
  return val;
}

uint64_t PackRowPosition(const RowPosition& rp) noexcept {
  return (static_cast<uint64_t>(rp.page_id) << 32) |
         static_cast<uint64_t>(static_cast<uint32_t>(rp.slot));
}

void AppendUint32(std::string* dst, uint32_t v) {
  const size_t pos = dst->size();
  dst->resize(pos + sizeof(v));
  std::memcpy(dst->data() + pos, &v, sizeof(v));
}

void AppendUint64(std::string* dst, uint64_t v) {
  const size_t pos = dst->size();
  dst->resize(pos + sizeof(v));
  std::memcpy(dst->data() + pos, &v, sizeof(v));
}

void AppendInt64(std::string* dst, int64_t v) {
  const size_t pos = dst->size();
  dst->resize(pos + sizeof(v));
  std::memcpy(dst->data() + pos, &v, sizeof(v));
}

bool ReadUint32(std::string_view* src, uint32_t* out) noexcept {
  if (src->size() < sizeof(uint32_t)) {
    return false;
  }
  std::memcpy(out, src->data(), sizeof(uint32_t));
  src->remove_prefix(sizeof(uint32_t));
  return true;
}

bool ReadUint64(std::string_view* src, uint64_t* out) noexcept {
  if (src->size() < sizeof(uint64_t)) {
    return false;
  }
  std::memcpy(out, src->data(), sizeof(uint64_t));
  src->remove_prefix(sizeof(uint64_t));
  return true;
}

bool ReadInt64(std::string_view* src, int64_t* out) noexcept {
  if (src->size() < sizeof(int64_t)) {
    return false;
  }
  std::memcpy(out, src->data(), sizeof(int64_t));
  src->remove_prefix(sizeof(int64_t));
  return true;
}

size_t EstimateKeyEntryBytes(size_t key_len) noexcept {
  return (sizeof(uint64_t) * 4) + key_len + 16;
}

Status ReserveOrFail(ProcessMemoryCharge* charge, size_t bytes) {
  if (bytes == 0) {
    return Status::kSuccess;
  }
  if (!charge->TryAddWithReclaim(bytes)) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "query memory limit exceeded in foreign key check");
  }
  return Status::kSuccess;
}

}  // namespace

std::string ForeignKeyString(const Value& value) {
  // Canonicalize ±0.0 / NaN / CI collation before encoding so the hash key
  // agrees with Value::operator== and EqualUnderCollation: 0.0 and -0.0 land
  // in the same bucket, CI-tagged VARCHARs fold to lowercase, NaNs canonicalize
  // to one pattern. This prevents false FK violations on INSERT/UPDATE and
  // dangling children on DELETE.
  Value canonical = relational_detail::CanonicalDistinctValue(value);
  // The leading type tag keeps INT64 1, DOUBLE 1.0 and VARCHAR "1" apart;
  // AsString is injective within one ValueType and DDL validates the child
  // and parent column types equal.
  return std::string(1, static_cast<char>(canonical.type)) +
         canonical.AsString();
}

void ForeignKeyCounts::SetBudgetBytesForTest(size_t bytes) noexcept {
  BudgetOverrideForTest().store(bytes, std::memory_order_release);
}

size_t ForeignKeyCounts::EffectiveBudgetBytes() noexcept {
  const size_t override_bytes =
      BudgetOverrideForTest().load(std::memory_order_acquire);
  if (override_bytes > 0) {
    return override_bytes;
  }
  size_t cap = kDefaultForeignKeyBudgetBytes;
  const size_t pool_max = ProcessMemoryBudget::Global().EffectivePoolMaxBytes(
      MemoryPoolKind::kQuery);
  if (pool_max > 0) {
    cap = std::min(cap, std::max<size_t>(size_t{4} << 10, pool_max / 4));
  }
  return cap;
}

ForeignKeyCounts::ForeignKeyCounts() : budget_bytes_(EffectiveBudgetBytes()) {}

ForeignKeyCounts::~ForeignKeyCounts() { ReleaseSpillSegment(); }

ForeignKeyCounts::ForeignKeyCounts(ForeignKeyCounts&& other) noexcept
    : budget_bytes_(other.budget_bytes_),
      spilled_(other.spilled_),
      counts_(std::move(other.counts_)),
      in_mem_bytes_(other.in_mem_bytes_),
      delta_counts_(std::move(other.delta_counts_)),
      delta_bytes_(other.delta_bytes_),
      mem_charge_(std::move(other.mem_charge_)),
      store_(std::move(other.store_)),
      segment_id_(other.segment_id_),
      two_level_(other.two_level_),
      top_metas_(std::move(other.top_metas_)) {
  other.segment_id_ = kInvalidSpillSegmentId;
  other.spilled_ = false;
  other.in_mem_bytes_ = 0;
  other.delta_bytes_ = 0;
  other.two_level_ = false;
}

ForeignKeyCounts& ForeignKeyCounts::operator=(
    ForeignKeyCounts&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  ReleaseSpillSegment();
  budget_bytes_ = other.budget_bytes_;
  spilled_ = other.spilled_;
  counts_ = std::move(other.counts_);
  in_mem_bytes_ = other.in_mem_bytes_;
  delta_counts_ = std::move(other.delta_counts_);
  delta_bytes_ = other.delta_bytes_;
  mem_charge_ = std::move(other.mem_charge_);
  store_ = std::move(other.store_);
  segment_id_ = other.segment_id_;
  two_level_ = other.two_level_;
  top_metas_ = std::move(other.top_metas_);
  other.segment_id_ = kInvalidSpillSegmentId;
  other.spilled_ = false;
  other.in_mem_bytes_ = 0;
  other.delta_bytes_ = 0;
  other.two_level_ = false;
  return *this;
}

void ForeignKeyCounts::ReleaseSpillSegment() noexcept {
  if (store_ != nullptr && segment_id_ != kInvalidSpillSegmentId) {
    // DropSegment is noexcept and returns void, so there is no Status to
    // propagate across this noexcept boundary (destructor / move assignment).
    // NOLINTNEXTLINE(readability-redundant-casting) cast documents intent
    (void)store_->DropSegment(segment_id_);
  }
  segment_id_ = kInvalidSpillSegmentId;
  top_metas_.clear();
  two_level_ = false;
}

Status ForeignKeyCounts::BuildSortedSegment(
    const std::function<Status(
        const std::function<Status(std::string_view, int64_t)>&)>& producer) {
  if (store_ == nullptr) {
    store_ = std::make_shared<SpillStore>();
  }
  ExternalSorter::Options opts;
  opts.spill_store = store_.get();
  opts.memory_budget_bytes = std::max<size_t>(size_t{16} << 10, budget_bytes_);
  opts.keys = {OocSortKey{.column_index = 0,
                          .ascending = true,
                          .nulls_first = std::nullopt,
                          .collation = Collation::kNone}};
  opts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled() ||
                     budget_bytes_ < (size_t{64} << 10);
  ExternalSorter sorter(std::move(opts));

  RETURN_IF_FAIL(
      producer([&](std::string_view key_sv, int64_t count) -> Status {
        return sorter.AddRow(Row({Value(std::string(key_sv)), Value(count)}));
      }));
  RETURN_IF_FAIL(sorter.Finish());

  auto seg_or = store_->AllocateSegment();
  RETURN_IF_FAIL(seg_or.GetStatus());
  const SpillSegmentId new_seg = seg_or.Value();
  uint64_t seg_offset = 0;

  std::vector<LeafMeta> leaf_metas;
  std::string block_buf;
  block_buf.reserve(kSortedLeafTargetBytes);
  std::string block_first_key;
  uint32_t block_entries = 0;

  auto flush_leaf = [&]() -> Status {
    if (block_entries == 0) {
      return Status::kSuccess;
    }
    // Validate the framed size before allocating or copying: under a tight
    // memory budget a huge leaf must be rejected before it is duplicated into
    // `framed`, otherwise the check itself comes too late to avoid the OOM.
    const size_t framed_size = sizeof(uint32_t) + block_buf.size();
    if (!std::in_range<uint32_t>(framed_size)) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "foreign key spill leaf exceeds the file format limit");
    }
    std::string framed;
    framed.reserve(framed_size);
    AppendUint32(&framed, block_entries);
    framed.append(block_buf);
    Status st = store_->Append(new_seg, framed.data(), framed.size());
    if (st != Status::kSuccess) {
      return st;
    }
    leaf_metas.push_back(LeafMeta{
        .first_key = std::move(block_first_key),
        .offset = seg_offset,
        .length = static_cast<uint32_t>(framed.size()),
    });
    seg_offset += framed.size();
    block_buf.clear();
    block_first_key.clear();
    block_entries = 0;
    return Status::kSuccess;
  };

  std::string cur_key;
  int64_t cur_count = 0;
  bool has_cur = false;
  auto emit_merged = [&](const std::string& key, int64_t count) -> Status {
    if (count <= 0) {
      return Status::kSuccess;
    }
    if (block_entries > 0 && sizeof(uint32_t) + block_buf.size() +
                                     sizeof(uint32_t) + key.size() +
                                     sizeof(int64_t) >
                                 kSortedLeafTargetBytes) {
      RETURN_IF_FAIL(flush_leaf());
    }
    if (block_entries == 0) {
      block_first_key = key;
    }
    AppendUint32(&block_buf, static_cast<uint32_t>(key.size()));
    block_buf.append(key);
    AppendInt64(&block_buf, count);
    ++block_entries;
    return Status::kSuccess;
  };

  Row sorted_row;
  while (true) {
    ASSIGN_OR_RETURN(bool, has_row, sorter.NextRow(&sorted_row));
    if (!has_row) {
      break;
    }
    const std::string_view k = sorted_row[0].AsStringView();
    const int64_t cnt = sorted_row[1].AsInt64();
    if (!has_cur) {
      cur_key.assign(k.data(), k.size());
      cur_count = cnt;
      has_cur = true;
    } else if (k == cur_key) {
      cur_count += cnt;
    } else {
      const Status emit_st = emit_merged(cur_key, cur_count);
      if (emit_st != Status::kSuccess) {
        store_->DropSegment(new_seg);
        return emit_st;
      }
      cur_key.assign(k.data(), k.size());
      cur_count = cnt;
    }
  }
  if (has_cur) {
    const Status emit_st = emit_merged(cur_key, cur_count);
    if (emit_st != Status::kSuccess) {
      store_->DropSegment(new_seg);
      return emit_st;
    }
  }
  Status last_st = flush_leaf();
  if (last_st != Status::kSuccess) {
    store_->DropSegment(new_seg);
    return last_st;
  }

  std::vector<LeafMeta> next_top;
  bool next_two_level = false;
  if (leaf_metas.size() <= kMaxTopLevelMetas) {
    next_top = std::move(leaf_metas);
  } else {
    next_two_level = true;
    const size_t entries_per_dir =
        (leaf_metas.size() + kMaxTopLevelMetas - 1) / kMaxTopLevelMetas;
    next_top.reserve(kMaxTopLevelMetas);
    for (size_t i = 0; i < leaf_metas.size(); i += entries_per_dir) {
      const size_t end = std::min(leaf_metas.size(), i + entries_per_dir);
      std::string dir_buf;
      dir_buf.reserve((end - i) * 16);
      AppendUint32(&dir_buf, static_cast<uint32_t>(end - i));
      for (size_t j = i; j < end; ++j) {
        const LeafMeta& lm = leaf_metas[j];
        AppendUint32(&dir_buf, static_cast<uint32_t>(lm.first_key.size()));
        dir_buf.append(lm.first_key);
        AppendUint64(&dir_buf, lm.offset);
        AppendUint32(&dir_buf, lm.length);
      }
      if (!std::in_range<uint32_t>(dir_buf.size())) {
        // DropSegment discards the whole segment, including the leaf pages
        // already appended above (leaf data and directory pages share new_seg).
        // Callers must therefore allocate a fresh segment on retry -- reusing
        // this id would leave the old leaves orphaned/reused. No leaf survives
        // a DropSegment call.
        store_->DropSegment(new_seg);
        return StatusError(
            StatusCode::kInvalidArgument,
            "foreign key spill directory exceeds the file format limit");
      }
      const Status dst =
          store_->Append(new_seg, dir_buf.data(), dir_buf.size());
      if (dst != Status::kSuccess) {
        store_->DropSegment(new_seg);
        return dst;
      }
      next_top.push_back(LeafMeta{
          .first_key = leaf_metas[i].first_key,
          .offset = seg_offset,
          .length = static_cast<uint32_t>(dir_buf.size()),
      });
      seg_offset += dir_buf.size();
    }
  }

  ReleaseSpillSegment();
  segment_id_ = new_seg;
  top_metas_ = std::move(next_top);
  two_level_ = next_two_level;
  spilled_ = true;
  return Status::kSuccess;
}

Status ForeignKeyCounts::SpillInMemoryToSortedSegment() {
  std::unordered_map<std::string, int64_t> old_counts;
  old_counts.swap(counts_);
  const size_t old_bytes = in_mem_bytes_;
  in_mem_bytes_ = 0;
  mem_charge_.Release(old_bytes);
  return BuildSortedSegment(
      [&](const std::function<Status(std::string_view, int64_t)>& emit)
          -> Status {
        for (const auto& [k, c] : old_counts) {
          RETURN_IF_FAIL(emit(k, c));
        }
        return Status::kSuccess;
      });
}

Status ForeignKeyCounts::RebuildWithDeltas() {
  const SpillSegmentId old_seg = segment_id_;
  std::vector<LeafMeta> old_top = std::move(top_metas_);
  const bool old_two_level = two_level_;
  std::unordered_map<std::string, int64_t> old_deltas;
  old_deltas.swap(delta_counts_);
  const size_t old_delta_bytes = delta_bytes_;
  delta_bytes_ = 0;
  mem_charge_.Release(old_delta_bytes);

  segment_id_ = kInvalidSpillSegmentId;
  top_metas_.clear();
  two_level_ = false;

  auto decode_leaf_block =
      [&](uint64_t off, uint32_t len,
          const std::function<Status(std::string_view, int64_t)>& emit)
      -> Status {
    std::string buf(len, '\0');
    RETURN_IF_FAIL(store_->Read(old_seg, off, buf.data(), len));
    std::string_view sv(buf);
    uint32_t n = 0;
    if (!ReadUint32(&sv, &n)) {
      return StatusError(StatusCode::kCorrupt, "corrupt FK leaf header");
    }
    for (uint32_t i = 0; i < n; ++i) {
      uint32_t klen = 0;
      if (!ReadUint32(&sv, &klen) || sv.size() < klen) {
        return StatusError(StatusCode::kCorrupt, "corrupt FK leaf key");
      }
      const std::string_view key = sv.substr(0, klen);
      sv.remove_prefix(klen);
      int64_t cnt = 0;
      if (!ReadInt64(&sv, &cnt)) {
        return StatusError(StatusCode::kCorrupt, "corrupt FK leaf count");
      }
      RETURN_IF_FAIL(emit(key, cnt));
    }
    return Status::kSuccess;
  };

  const Status build_st = BuildSortedSegment(
      [&](const std::function<Status(std::string_view, int64_t)>& emit)
          -> Status {
        if (old_seg != kInvalidSpillSegmentId) {
          for (const LeafMeta& top : old_top) {
            if (!old_two_level) {
              RETURN_IF_FAIL(decode_leaf_block(top.offset, top.length, emit));
            } else {
              // T4637: Read fills every byte; skip the memset.
              std::unique_ptr<char[]>  // NOLINT(modernize-avoid-c-arrays): runtime-sized read owner.
              dir_raw(  // NOLINT(modernize-avoid-c-arrays):
                        // runtime-sized read buffer.
                  new char[top.length]);
              RETURN_IF_FAIL(
                  store_->Read(old_seg, top.offset, dir_raw.get(), top.length));
              std::string_view sv(dir_raw.get(), top.length);
              uint32_t n = 0;
              if (!ReadUint32(&sv, &n)) {
                return StatusError(StatusCode::kCorrupt,
                                   "corrupt FK dir header");
              }
              for (uint32_t i = 0; i < n; ++i) {
                uint32_t klen = 0;
                uint64_t loff = 0;
                uint32_t llen = 0;
                if (!ReadUint32(&sv, &klen) || sv.size() < klen) {
                  return StatusError(StatusCode::kCorrupt,
                                     "corrupt FK dir key");
                }
                sv.remove_prefix(klen);
                if (!ReadUint64(&sv, &loff) || !ReadUint32(&sv, &llen)) {
                  return StatusError(StatusCode::kCorrupt,
                                     "corrupt FK dir offset");
                }
                RETURN_IF_FAIL(decode_leaf_block(loff, llen, emit));
              }
            }
          }
        }
        for (const auto& [k, c] : old_deltas) {
          RETURN_IF_FAIL(emit(k, c));
        }
        return Status::kSuccess;
      });

  if (old_seg != kInvalidSpillSegmentId && store_ != nullptr) {
    store_->DropSegment(old_seg);
  }
  return build_st;
}

StatusOr<int64_t> ForeignKeyCounts::LookupSpilledCount(
    std::string_view key) const {
  if (top_metas_.empty() || store_ == nullptr ||
      segment_id_ == kInvalidSpillSegmentId) {
    return int64_t{0};
  }
  auto find_meta = [](const std::vector<LeafMeta>& metas,
                      std::string_view k) -> const LeafMeta* {
    auto it = std::upper_bound(metas.begin(), metas.end(), k,
                               [](std::string_view needle, const LeafMeta& m) {
                                 return needle < m.first_key;
                               });
    if (it == metas.begin()) {
      return nullptr;
    }
    return &*std::prev(it);
  };

  const LeafMeta* leaf = find_meta(top_metas_, key);
  if (leaf == nullptr) {
    return int64_t{0};
  }

  uint64_t leaf_off = leaf->offset;
  uint32_t leaf_len = leaf->length;
  if (two_level_) {
    // T4636: store_->Read overwrites every byte; the old
    // std::string(leaf->length, '\0') memset the same block again for
    // nothing. Default-new'ed char[] skips it, view keeps parsing identical.
    // NOLINTNEXTLINE(modernize-avoid-c-arrays): runtime-sized read buffer.
    std::unique_ptr<char[]> dir_raw(new char[leaf->length]);
    RETURN_IF_FAIL(
        store_->Read(segment_id_, leaf->offset, dir_raw.get(), leaf->length));
    std::string_view sv(dir_raw.get(), leaf->length);
    uint32_t n = 0;
    if (!ReadUint32(&sv, &n)) {
      return StatusError(StatusCode::kCorrupt, "corrupt FK dir header");
    }
    uint64_t chosen_off = 0;
    uint32_t chosen_len = 0;
    bool found_dir = false;
    for (uint32_t i = 0; i < n; ++i) {
      uint32_t klen = 0;
      uint64_t loff = 0;
      uint32_t llen = 0;
      if (!ReadUint32(&sv, &klen) || sv.size() < klen) {
        return StatusError(StatusCode::kCorrupt, "corrupt FK dir entry");
      }
      const std::string_view fk = sv.substr(0, klen);
      sv.remove_prefix(klen);
      if (!ReadUint64(&sv, &loff) || !ReadUint32(&sv, &llen)) {
        return StatusError(StatusCode::kCorrupt, "corrupt FK dir pointer");
      }
      if (fk <= key) {
        chosen_off = loff;
        chosen_len = llen;
        found_dir = true;
      } else {
        break;
      }
    }
    if (!found_dir) {
      return int64_t{0};
    }
    leaf_off = chosen_off;
    leaf_len = chosen_len;
  }

  // T4635: same uninit-buffer rule as the dir read above.
  // NOLINTNEXTLINE(modernize-avoid-c-arrays): runtime-sized read buffer.
  std::unique_ptr<char[]> leaf_raw(new char[leaf_len]);
  RETURN_IF_FAIL(store_->Read(segment_id_, leaf_off, leaf_raw.get(), leaf_len));
  std::string_view sv(leaf_raw.get(), leaf_len);
  uint32_t n = 0;
  if (!ReadUint32(&sv, &n)) {
    return StatusError(StatusCode::kCorrupt, "corrupt FK leaf block");
  }
  struct Entry {
    std::string_view key;
    int64_t count;
  };
  std::vector<Entry> entries;
  entries.reserve(n);
  for (uint32_t i = 0; i < n; ++i) {
    uint32_t klen = 0;
    if (!ReadUint32(&sv, &klen) || sv.size() < klen) {
      return StatusError(StatusCode::kCorrupt, "corrupt FK leaf key");
    }
    const std::string_view k = sv.substr(0, klen);
    sv.remove_prefix(klen);
    int64_t cnt = 0;
    if (!ReadInt64(&sv, &cnt)) {
      return StatusError(StatusCode::kCorrupt, "corrupt FK leaf count");
    }
    entries.push_back({k, cnt});
  }
  auto lb = std::lower_bound(
      entries.begin(), entries.end(), key,
      [](const Entry& e, std::string_view needle) { return e.key < needle; });
  if (lb != entries.end() && lb->key == key) {
    return lb->count;
  }
  return int64_t{0};
}

Status ForeignKeyCounts::IncrementKey(const Value& key) {
  if (key.IsNull()) {
    return Status::kSuccess;
  }
  std::string encoded = ForeignKeyString(key);
  if (!spilled_) {
    auto it = counts_.find(encoded);
    if (it != counts_.end()) {
      ++it->second;
      return Status::kSuccess;
    }
    const size_t entry_bytes = EstimateKeyEntryBytes(encoded.size());
    if (!ProcessMemoryBudget::Global().AlwaysSpillEnabled() &&
        in_mem_bytes_ + entry_bytes <= budget_bytes_ &&
        mem_charge_.TryAddWithReclaim(entry_bytes)) {
      counts_.emplace(std::move(encoded), 1);
      in_mem_bytes_ += entry_bytes;
      return Status::kSuccess;
    }
    // This key was NOT charged (AlwaysSpill, over-budget, or a failed
    // TryAddWithReclaim). It still has to be written into the sorted segment,
    // but its bytes must not be added to in_mem_bytes_: the spill path
    // releases the whole counter from mem_charge_, so counting uncharged
    // bytes here would under-flow the process-wide budget.
    counts_.emplace(std::move(encoded), 1);
    return SpillInMemoryToSortedSegment();
  }
  return ApplySpilledDelta(std::move(encoded), 1);
}

Status ForeignKeyCounts::DecrementKey(const Value& key) {
  if (key.IsNull()) {
    return Status::kSuccess;
  }
  std::string encoded = ForeignKeyString(key);
  if (!spilled_) {
    auto found = counts_.find(encoded);
    if (found != counts_.end() && --found->second <= 0) {
      const size_t entry_bytes = EstimateKeyEntryBytes(found->first.size());
      counts_.erase(found);
      in_mem_bytes_ =
          in_mem_bytes_ > entry_bytes ? in_mem_bytes_ - entry_bytes : 0;
      mem_charge_.Release(entry_bytes);
    }
    return Status::kSuccess;
  }
  return ApplySpilledDelta(std::move(encoded), -1);
}

Status ForeignKeyCounts::ApplySpilledDelta(std::string encoded, int64_t delta) {
  auto found = delta_counts_.find(encoded);
  if (found != delta_counts_.end()) {
    found->second += delta;
    return Status::kSuccess;
  }
  const size_t entry_bytes = EstimateKeyEntryBytes(encoded.size());
  RETURN_IF_FAIL(ReserveOrFail(&mem_charge_, entry_bytes));
  delta_counts_.emplace(std::move(encoded), delta);
  delta_bytes_ += entry_bytes;
  if (delta_bytes_ > std::max<size_t>(4096, budget_bytes_ / 2)) {
    return RebuildWithDeltas();
  }
  return Status::kSuccess;
}

StatusOr<bool> ForeignKeyCounts::Contains(const Value& key) const {
  if (key.IsNull()) {
    return false;
  }
  const std::string encoded = ForeignKeyString(key);
  if (!spilled_) {
    const auto found = counts_.find(encoded);
    return found != counts_.end() && found->second > 0;
  }
  int64_t delta = 0;
  if (!delta_counts_.empty()) {
    const auto dfound = delta_counts_.find(encoded);
    if (dfound != delta_counts_.end()) {
      delta = dfound->second;
    }
  }
  ASSIGN_OR_RETURN(int64_t, base, LookupSpilledCount(encoded));
  return (base + delta) > 0;
}

StatusOr<ForeignKeyCounts> BuildForeignKeyCounts(Table& parent,
                                                 Transaction& txn,
                                                 size_t parent_offset) {
  ForeignKeyCounts counts;
  const size_t budget = counts.budget_bytes_;
  const bool force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();

  std::vector<slot_t> projection = {static_cast<slot_t>(parent_offset)};
  auto it = parent.BeginFullScan(txn, projection);
  bool use_sorter = force_spill;

  if (!use_sorter) {
    uint64_t scan_steps = 0;
    for (; it.IsValid(); ++it) {
      if ((++scan_steps & 255U) == 0 && txn.HasStatementTimeout()) {
        RETURN_IF_FAIL(txn.CheckStatementTimeout());
      }
      const Row& row = *it;
      if (row.values_.empty() || row[0].IsNull()) {
        continue;
      }
      std::string encoded = ForeignKeyString(row[0]);
      auto found = counts.counts_.find(encoded);
      if (found != counts.counts_.end()) {
        ++found->second;
        continue;
      }
      const size_t entry_bytes = EstimateKeyEntryBytes(encoded.size());
      if (counts.in_mem_bytes_ + entry_bytes > budget ||
          !counts.mem_charge_.TryAddWithReclaim(entry_bytes)) {
        use_sorter = true;
        break;
      }
      counts.counts_.emplace(std::move(encoded), 1);
      counts.in_mem_bytes_ += entry_bytes;
    }
    RETURN_IF_FAIL(it.GetStatus());
    if (!use_sorter) {
      return counts;
    }
  }

  std::unordered_map<std::string, int64_t> prefix_counts;
  prefix_counts.swap(counts.counts_);
  const size_t rel = counts.in_mem_bytes_;
  counts.in_mem_bytes_ = 0;
  counts.mem_charge_.Release(rel);

  RETURN_IF_FAIL(counts.BuildSortedSegment(
      [&](const std::function<Status(std::string_view, int64_t)>& emit)
          -> Status {
        for (const auto& [k, c] : prefix_counts) {
          RETURN_IF_FAIL(emit(k, c));
        }
        prefix_counts.clear();
        for (; it.IsValid(); ++it) {
          const Row& row = *it;
          if (row.values_.empty() || row[0].IsNull()) {
            continue;
          }
          RETURN_IF_FAIL(emit(ForeignKeyString(row[0]), 1));
        }
        return it.GetStatus();
      }));
  return counts;
}

StatusOr<bool> IsForeignKeyViolation(const ForeignKeyCounts& counts,
                                     const Value& key) {
  if (key.IsNull()) {
    return false;
  }
  ASSIGN_OR_RETURN(bool, found, counts.Contains(key));
  return !found;
}

// ---- SpillableRowPositionList ----

SpillableRowPositionList::SpillableRowPositionList()
    : budget_bytes_(std::max<size_t>(
          size_t{4} << 10, ForeignKeyCounts::EffectiveBudgetBytes() / 4)) {}

SpillableRowPositionList::~SpillableRowPositionList() {
  if (writer_ != nullptr) {
    const SpillSegmentId sid = writer_->SegmentId();
    writer_.reset();
    if (store_ != nullptr && sid != kInvalidSpillSegmentId) {
      store_->DropSegment(sid);
    }
  }
}

SpillableRowPositionList::SpillableRowPositionList(
    SpillableRowPositionList&& other) noexcept = default;
SpillableRowPositionList& SpillableRowPositionList::operator=(
    SpillableRowPositionList&& other) noexcept = default;

Status SpillableRowPositionList::FlushBatchToSpill(const RowPosition* data,
                                                   size_t n) {
  if (n == 0) {
    return Status::kSuccess;
  }
  if (writer_ == nullptr) {
    store_ = std::make_shared<SpillStore>();
    writer_ =
        std::make_unique<SpillSegmentWriter>(store_.get(), size_t{16} << 10);
  }
  return writer_->Write(data, n * sizeof(RowPosition));
}

Status SpillableRowPositionList::TransitionToSpill() {
  if (spilled_) {
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(FlushBatchToSpill(in_memory_.data(), in_memory_.size()));
  in_memory_.clear();
  in_memory_.shrink_to_fit();
  mem_charge_.ReleaseAll();
  spilled_ = true;
  return Status::kSuccess;
}

Status SpillableRowPositionList::PushBack(const RowPosition& pos) {
  const bool force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  if (!spilled_) {
    const size_t add_bytes = sizeof(RowPosition);
    if ((force_spill && count_ >= 1) ||
        (in_memory_.size() + 1) * sizeof(RowPosition) > budget_bytes_ ||
        !mem_charge_.TryAddWithReclaim(add_bytes)) {
      RETURN_IF_FAIL(TransitionToSpill());
    } else {
      in_memory_.push_back(pos);
      ++count_;
      return Status::kSuccess;
    }
  }
  // Post-spill staging tail: TransitionToSpill() released the whole charge,
  // so without a budget check here the tail silently regrows to
  // kRowPosFlushBatch entries (16 KiB) that are neither charged nor bounded
  // by budget_bytes_ (which bottoms out at 4 KiB). Keep it both charged and
  // bounded, flushing before it can exceed the budget.
  const size_t add_bytes = sizeof(RowPosition);
  const size_t tail_limit = std::max<size_t>(
      1,
      std::min<size_t>(kRowPosFlushBatch, budget_bytes_ / sizeof(RowPosition)));
  if (!mem_charge_.TryAddWithReclaim(add_bytes) ||
      in_memory_.size() >= tail_limit || force_spill) {
    // Release the staged-row charge on *every* exit from this block.  A failed
    // flush used to RETURN_IF_FAIL before ReleaseAll (and a failed second
    // reclaim likewise left the first attempt's charge live), leaking budget
    // until the query ended.  Capture the status, release, then return it.
    const Status flush_status =
        FlushBatchToSpill(in_memory_.data(), in_memory_.size());
    in_memory_.clear();
    mem_charge_.ReleaseAll();
    RETURN_IF_FAIL(flush_status);
    if (!mem_charge_.TryAddWithReclaim(add_bytes)) {
      // No room even for one staged row: write it straight to the spill file.
      RETURN_IF_FAIL(FlushBatchToSpill(&pos, 1));
      ++count_;
      return Status::kSuccess;
    }
  }
  in_memory_.push_back(pos);
  ++count_;
  return Status::kSuccess;
}

Status SpillableRowPositionList::ForEach(
    const std::function<Status(const RowPosition&)>& fn) const {
  if (!spilled_) {
    for (const RowPosition& pos : in_memory_) {
      RETURN_IF_FAIL(fn(pos));
    }
    return Status::kSuccess;
  }
  if (writer_ != nullptr) {
    RETURN_IF_FAIL(const_cast<SpillSegmentWriter*>(writer_.get())->Flush());
    SpillSegmentReader reader(store_.get(), writer_->SegmentId(),
                              size_t{16} << 10);
    RETURN_IF_FAIL(reader.Open());
    std::vector<RowPosition> chunk(kRowPosFlushBatch);
    while (!reader.Eof()) {
      ASSIGN_OR_RETURN(
          size_t, read_bytes,
          reader.ReadUpTo(chunk.data(), chunk.size() * sizeof(RowPosition)));
      if (read_bytes == 0) {
        break;
      }
      if (read_bytes % sizeof(RowPosition) != 0) {
        return StatusError(StatusCode::kCorrupt,
                           "truncated RowPosition spill record");
      }
      const size_t n = read_bytes / sizeof(RowPosition);
      for (size_t i = 0; i < n; ++i) {
        RETURN_IF_FAIL(fn(chunk[i]));
      }
    }
  }
  for (const RowPosition& pos : in_memory_) {
    RETURN_IF_FAIL(fn(pos));
  }
  return Status::kSuccess;
}

// ---- SpillablePendingUpdateList ----

SpillablePendingUpdateList::SpillablePendingUpdateList()
    : budget_bytes_(std::max<size_t>(
          size_t{8} << 10, ForeignKeyCounts::EffectiveBudgetBytes() / 2)) {}

SpillablePendingUpdateList::~SpillablePendingUpdateList() {
  if (writer_ != nullptr) {
    const SpillSegmentId sid = writer_->SegmentId();
    writer_.reset();
    if (store_ != nullptr && sid != kInvalidSpillSegmentId) {
      store_->DropSegment(sid);
    }
  }
}

SpillablePendingUpdateList::SpillablePendingUpdateList(
    SpillablePendingUpdateList&& other) noexcept = default;
SpillablePendingUpdateList& SpillablePendingUpdateList::operator=(
    SpillablePendingUpdateList&& other) noexcept = default;

Status SpillablePendingUpdateList::WriteEntryToSpill(const Row& row,
                                                     const RowPosition& pos) {
  if (writer_ == nullptr) {
    store_ = std::make_shared<SpillStore>();
    writer_ = std::make_unique<SpillSegmentWriter>(store_.get(),
                                                   kUpdateBlockTargetBytes);
  }
  scratch_.clear();
  RETURN_IF_FAIL(EncodeSpillRow(row, &scratch_));
  const auto row_bytes_len = static_cast<uint32_t>(scratch_.size());
  RETURN_IF_FAIL(writer_->WritePod(pos));
  RETURN_IF_FAIL(writer_->WritePod(row_bytes_len));
  return writer_->Write(scratch_.data(), scratch_.size());
}

Status SpillablePendingUpdateList::TransitionToSpill() {
  if (spilled_) {
    return Status::kSuccess;
  }
  for (const auto& [row, pos] : in_memory_) {
    RETURN_IF_FAIL(WriteEntryToSpill(row, pos));
  }
  std::vector<std::pair<Row, RowPosition>>().swap(in_memory_);
  in_mem_bytes_ = 0;
  mem_charge_.ReleaseAll();
  spilled_ = true;
  return Status::kSuccess;
}

Status SpillablePendingUpdateList::PushBack(Row row, RowPosition pos) {
  const bool force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  if (!spilled_) {
    const size_t entry_bytes =
        EstimateRowMemoryBytes(row) + sizeof(RowPosition);
    if ((force_spill && count_ >= 1) ||
        in_mem_bytes_ + entry_bytes > budget_bytes_ ||
        !mem_charge_.TryAddWithReclaim(entry_bytes)) {
      RETURN_IF_FAIL(TransitionToSpill());
    } else {
      in_memory_.emplace_back(std::move(row), pos);
      in_mem_bytes_ += entry_bytes;
      ++count_;
      return Status::kSuccess;
    }
  }
  ++count_;
  return WriteEntryToSpill(row, pos);
}

Status SpillablePendingUpdateList::ForEach(
    const std::function<Status(const Row&, const RowPosition&)>& fn) const {
  if (!spilled_) {
    for (const auto& [row, pos] : in_memory_) {
      RETURN_IF_FAIL(fn(row, pos));
    }
    return Status::kSuccess;
  }
  if (writer_ != nullptr) {
    RETURN_IF_FAIL(const_cast<SpillSegmentWriter*>(writer_.get())->Flush());
    SpillSegmentReader reader(store_.get(), writer_->SegmentId(),
                              kUpdateBlockTargetBytes);
    RETURN_IF_FAIL(reader.Open());
    std::string buf;
    for (size_t i = 0; i < count_; ++i) {
      RowPosition pos{};
      uint32_t row_len = 0;
      RETURN_IF_FAIL(reader.ReadPod(&pos));
      RETURN_IF_FAIL(reader.ReadPod(&row_len));
      buf.resize(row_len);
      if (row_len > 0) {
        RETURN_IF_FAIL(reader.ReadExact(buf.data(), row_len));
      }
      std::string_view sv(buf);
      ASSIGN_OR_RETURN(Row, row, DecodeSpillRow(&sv));
      RETURN_IF_FAIL(fn(row, pos));
    }
  }
  return Status::kSuccess;
}

// ---- Batch / ExternalSorter-backed parent-reference check ----

StatusOr<std::optional<std::string>> FindReferencedParentKey(
    Table& child, Transaction& txn, size_t child_offset,
    const std::function<
        Status(const std::function<Status(const Value& parent_key)>& emit)>&
        for_each_candidate_key,
    const std::function<
        Status(const std::function<Status(const RowPosition& pos)>& emit)>&
        for_each_ignore_pos) {
  const size_t budget = ForeignKeyCounts::EffectiveBudgetBytes();
  const bool force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();

  std::unordered_map<std::string, std::string> in_mem_candidates;
  size_t cand_bytes = 0;
  std::unordered_set<uint64_t> ignore_packed;
  size_t ignore_bytes = 0;
  ProcessMemoryCharge mem_charge(MemoryPoolKind::kQuery, 0, false);

  bool spill_mode = force_spill;
  if (!spill_mode) {
    const Status cand_st =
        for_each_candidate_key([&](const Value& val) -> Status {
          if (val.IsNull()) {
            return Status::kSuccess;
          }
          std::string enc = ForeignKeyString(val);
          if (in_mem_candidates.contains(enc)) {
            return Status::kSuccess;
          }
          const std::string disp = val.AsString();
          const size_t add = EstimateKeyEntryBytes(enc.size() + disp.size());
          if (cand_bytes + add > budget / 2 ||
              !mem_charge.TryAddWithReclaim(add)) {
            spill_mode = true;
            return Status::kSuccess;
          }
          cand_bytes += add;
          in_mem_candidates.emplace(std::move(enc), disp);
          return Status::kSuccess;
        });
    RETURN_IF_FAIL(cand_st);
    if (!spill_mode && in_mem_candidates.empty()) {
      return std::optional<std::string>(std::nullopt);
    }
    if (!spill_mode) {
      const Status ign_st =
          for_each_ignore_pos([&](const RowPosition& pos) -> Status {
            const size_t add = sizeof(uint64_t) * 2;
            if (ignore_bytes + add > budget / 4 ||
                !mem_charge.TryAddWithReclaim(add)) {
              spill_mode = true;
              return Status::kSuccess;
            }
            if (ignore_packed.insert(PackRowPosition(pos)).second) {
              ignore_bytes += add;
            }
            return Status::kSuccess;
          });
      RETURN_IF_FAIL(ign_st);
    }
  }

  if (!spill_mode) {
    std::vector<slot_t> projection = {static_cast<slot_t>(child_offset)};
    auto it = child.BeginFullScan(txn, projection);
    for (; it.IsValid(); ++it) {
      if (!ignore_packed.empty() &&
          ignore_packed.contains(PackRowPosition(it.Position()))) {
        continue;
      }
      const Row& row = *it;
      if (row.values_.empty() || row[0].IsNull()) {
        continue;
      }
      const auto found = in_mem_candidates.find(ForeignKeyString(row[0]));
      if (found != in_mem_candidates.end()) {
        return std::optional<std::string>(found->second);
      }
    }
    RETURN_IF_FAIL(it.GetStatus());
    return std::optional<std::string>(std::nullopt);
  }

  // OOC path via ExternalSorter.
  in_mem_candidates.clear();
  ignore_packed.clear();
  mem_charge.ReleaseAll();

  SpillStore store;
  const size_t per_sorter_budget =
      std::max<size_t>(size_t{16} << 10, budget / 2);

  ExternalSorter::Options cand_opts;
  cand_opts.spill_store = &store;
  cand_opts.memory_budget_bytes = per_sorter_budget;
  cand_opts.keys = {OocSortKey{.column_index = 0,
                               .ascending = true,
                               .nulls_first = std::nullopt,
                               .collation = Collation::kNone}};
  cand_opts.force_spill = force_spill || budget < (size_t{64} << 10);
  ExternalSorter candidate_sorter(std::move(cand_opts));

  RETURN_IF_FAIL(for_each_candidate_key([&](const Value& val) -> Status {
    if (val.IsNull()) {
      return Status::kSuccess;
    }
    // T4638: only the encoded comparison key goes into the sorter now;
    // the old second column materialized val.AsString() for EVERY candidate
    // even though the message is consumed on the rare violation path.
    // ForeignKeyString is tag + canonical AsString, so the display form is
    // recovered by stripping the tag when (and only when) a key hits.
    return candidate_sorter.AddRow(Row({Value(ForeignKeyString(val))}));
  }));
  RETURN_IF_FAIL(candidate_sorter.Finish());
  if (candidate_sorter.TotalRows() == 0) {
    return std::optional<std::string>(std::nullopt);
  }

  // Check if ignore set fits in budget / 4; if not, perform an ExternalSorter
  // anti-join by RowPosition before sorting child keys.
  std::unordered_set<uint64_t> fast_ignore;
  size_t fast_ign_bytes = 0;
  bool ignore_spilled = false;
  if (!force_spill) {
    const Status ign_st =
        for_each_ignore_pos([&](const RowPosition& pos) -> Status {
          const size_t add = sizeof(uint64_t) * 2;
          if (fast_ign_bytes + add > budget / 4 ||
              !mem_charge.TryAddWithReclaim(add)) {
            ignore_spilled = true;
            return Status::kSuccess;
          }
          if (fast_ignore.insert(PackRowPosition(pos)).second) {
            fast_ign_bytes += add;
          }
          return Status::kSuccess;
        });
    RETURN_IF_FAIL(ign_st);
  } else {
    // Mark the spill path when any ignore position exists.
    RETURN_IF_FAIL(for_each_ignore_pos([&](const RowPosition&) -> Status {
      ignore_spilled = true;
      return Status::kSuccess;
    }));
  }

  ExternalSorter::Options child_opts;
  child_opts.spill_store = &store;
  child_opts.memory_budget_bytes = per_sorter_budget;
  child_opts.keys = {OocSortKey{.column_index = 0,
                                .ascending = true,
                                .nulls_first = std::nullopt,
                                .collation = Collation::kNone}};
  child_opts.force_spill = force_spill || budget < (size_t{64} << 10);
  ExternalSorter child_key_sorter(std::move(child_opts));

  std::vector<slot_t> projection = {static_cast<slot_t>(child_offset)};
  if (!ignore_spilled) {
    auto it = child.BeginFullScan(txn, projection);
    for (; it.IsValid(); ++it) {
      if (!fast_ignore.empty() &&
          fast_ignore.contains(PackRowPosition(it.Position()))) {
        continue;
      }
      const Row& row = *it;
      if (row.values_.empty() || row[0].IsNull()) {
        continue;
      }
      RETURN_IF_FAIL(
          child_key_sorter.AddRow(Row({Value(ForeignKeyString(row[0]))})));
    }
    RETURN_IF_FAIL(it.GetStatus());
    fast_ignore.clear();
    mem_charge.ReleaseAll();
  } else {
    fast_ignore.clear();
    mem_charge.ReleaseAll();

    ExternalSorter::Options anti_opts;
    anti_opts.spill_store = &store;
    anti_opts.memory_budget_bytes = per_sorter_budget;
    anti_opts.keys = {
        OocSortKey{.column_index = 0,
                   .ascending = true,
                   .nulls_first = std::nullopt,
                   .collation = Collation::kNone},
        OocSortKey{.column_index = 1,
                   .ascending = true,
                   .nulls_first = std::nullopt,
                   .collation = Collation::kNone},
        OocSortKey{.column_index = 2,
                   .ascending = true,
                   .nulls_first = std::nullopt,
                   .collation = Collation::kNone},
    };
    anti_opts.force_spill = force_spill || budget < (size_t{64} << 10);
    ExternalSorter anti_sorter(std::move(anti_opts));

    RETURN_IF_FAIL(for_each_ignore_pos([&](const RowPosition& pos) -> Status {
      return anti_sorter.AddRow(Row({Value(static_cast<int64_t>(pos.page_id)),
                                     Value(static_cast<int64_t>(pos.slot)),
                                     Value(int64_t{1}), Value("")}));
    }));
    auto it = child.BeginFullScan(txn, projection);
    for (; it.IsValid(); ++it) {
      const Row& row = *it;
      if (row.values_.empty() || row[0].IsNull()) {
        continue;
      }
      const RowPosition rp = it.Position();
      RETURN_IF_FAIL(anti_sorter.AddRow(
          Row({Value(static_cast<int64_t>(rp.page_id)),
               Value(static_cast<int64_t>(rp.slot)), Value(int64_t{2}),
               Value(ForeignKeyString(row[0]))})));
    }
    RETURN_IF_FAIL(it.GetStatus());
    RETURN_IF_FAIL(anti_sorter.Finish());

    uint64_t ignored_pos = kRowPosNullPackSentinel;
    Row anti_row;
    while (true) {
      ASSIGN_OR_RETURN(bool, has_anti, anti_sorter.NextRow(&anti_row));
      if (!has_anti) {
        break;
      }
      const uint64_t packed = PackRowPosition(
          RowPosition{static_cast<page_id_t>(anti_row[0].value.int_value),
                      static_cast<slot_t>(anti_row[1].value.int_value)});
      if (anti_row[2].value.int_value == 1) {
        ignored_pos = packed;
      } else if (packed != ignored_pos) {
        RETURN_IF_FAIL(child_key_sorter.AddRow(Row({anti_row[3]})));
      }
    }
  }
  RETURN_IF_FAIL(child_key_sorter.Finish());

  // 2-way merge join between sorted candidate keys and sorted child keys.
  Row cand_row;
  Row child_row;
  ASSIGN_OR_RETURN(bool, has_cand, candidate_sorter.NextRow(&cand_row));
  ASSIGN_OR_RETURN(bool, has_child, child_key_sorter.NextRow(&child_row));
  while (has_cand && has_child) {
    const std::string_view ck = cand_row[0].AsStringView();
    const std::string_view dk = child_row[0].AsStringView();
    if (ck == dk) {
      // T4638: display value is the encoded key minus its leading type tag
      // (canonicalized form of the compared value).
      return std::optional<std::string>(std::string(ck.substr(1)));
    }
    if (ck < dk) {
      ASSIGN_OR_RETURN(bool, next_cand, candidate_sorter.NextRow(&cand_row));
      has_cand = next_cand;
    } else {
      ASSIGN_OR_RETURN(bool, next_child, child_key_sorter.NextRow(&child_row));
      has_child = next_child;
    }
  }
  return std::optional<std::string>(std::nullopt);
}

StatusOr<bool> IsParentKeyReferenced(Table& child, Transaction& txn,
                                     size_t child_offset,
                                     const Value& parent_key,
                                     const std::vector<RowPosition>& ignore) {
  if (parent_key.IsNull()) {
    return false;
  }
  ASSIGN_OR_RETURN(
      std::optional<std::string>, ref,
      FindReferencedParentKey(
          child, txn, child_offset,
          [&](const std::function<Status(const Value&)>& emit) -> Status {
            return emit(parent_key);
          },
          [&](const std::function<Status(const RowPosition&)>& emit) -> Status {
            for (const RowPosition& pos : ignore) {
              RETURN_IF_FAIL(emit(pos));
            }
            return Status::kSuccess;
          }));
  return ref.has_value();
}

}  // namespace tinylamb
