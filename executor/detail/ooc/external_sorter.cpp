/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/platform/process.hpp"
#include "executor/detail/ooc/external_sorter.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

#include "common/memory_budget.hpp"
#include "type/collation.hpp"
#include "type/row_block_codec.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr size_t kWriteBatchRows = 256;
constexpr size_t kWriteBatchMaxBytes = 16ULL
                                       << 10;  // 16 KiB bounded decode unit

}  // namespace

// P34/61: loser tree over the merge streams (memory.md §3.4 requirement,
// previously a std::binary_heap over full Rows).  Leaves are stream indices
// padded to a power of two; `losers[0]` holds the current winner and
// `losers[1..K-1]` the parked loser of each internal match.  A pop replays
// the consumed leaf against the losers parked on its path, costing exactly
// log2(K) comparisons (the heap needed ~2 log2).  Players are compared via
// a caller-supplied `key_beats(a, b)` over the streams' cached key cells
// (already strict-total: equal keys break by lower stream index); -1 slots
// and inactive (exhausted) streams always lose.
struct ExternalSorter::LoserMerge {
  size_t K{0};
  std::vector<int> losers;
  std::vector<uint8_t> active;

  void SetActive(size_t i, bool is_active) { active[i] = is_active ? 1 : 0; }
  // -1 (all players exhausted / never inserted) ends the merge: an
  // exhausted leaf stays in the tree but can never be the winner.
  [[nodiscard]] int Top() const {
    if (losers.empty() || losers[0] < 0 ||
        active[static_cast<size_t>(losers[0])] == 0) {
      return -1;
    }
    return losers[0];
  }

  template <typename KeyBeats>
  [[nodiscard]] bool Better(int a, int b, const KeyBeats& key_beats) const {
    const bool a_live = a >= 0 && active[static_cast<size_t>(a)] != 0;
    const bool b_live = b >= 0 && active[static_cast<size_t>(b)] != 0;
    if (a_live != b_live) {
      return a_live;
    }
    if (!a_live) {
      return false;
    }
    return key_beats(a, b);
  }

  template <typename KeyBeats>
  void Build(size_t num_streams, const std::vector<uint8_t>& initial_active,
             const KeyBeats& key_beats) {
    K = 2;
    while (K < num_streams) {
      K <<= 1;
    }
    losers.assign(K, -1);
    active = initial_active;
    std::vector<int> win(2 * K, -1);
    for (size_t i = 0; i < num_streams; ++i) {
      win[K + i] = static_cast<int>(i);
    }
    for (size_t node = K - 1; node >= 1; --node) {
      const int a = win[2 * node];
      const int b = win[(2 * node) + 1];
      if (Better(a, b, key_beats)) {
        win[node] = a;
        losers[node] = b;
      } else {
        win[node] = b;
        losers[node] = a;
      }
    }
    losers[0] = win[1];
  }

  // Replays the path of stream `t` (its key changed or it was deactivated
  // via SetActive) after it was the winner.
  template <typename KeyBeats>
  void Replay(int t, const KeyBeats& key_beats) {
    if (t < 0) {
      return;
    }
    int cur = t;
    for (size_t node = (K + static_cast<size_t>(t)) >> 1; node >= 1;
         node >>= 1) {
      if (Better(losers[node], cur, key_beats)) {
        std::swap(cur, losers[node]);
      }
    }
    losers[0] = cur;
  }
};

ExternalSorter::ExternalSorter(Options options)
    : options_(std::move(options)),
      spill_store_alive_(options_.spill_store != nullptr
                             ? options_.spill_store->LifetimeToken()
                             : std::weak_ptr<const bool>{}) {
  key_orders_.reserve(options_.keys.size());
  for (const OocSortKey& k : options_.keys) {
    key_orders_.push_back(MakeSortKeyOrder(k));
  }
  consumer_guard_.emplace(options_.grant);
  if (options_.grant != nullptr) {
    const size_t refill =
        options_.memory_budget_bytes > 0
            ? std::min<size_t>(
                  QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                  std::max<size_t>(65536, options_.memory_budget_bytes / 4))
            : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
    grant_cache_.emplace(options_.grant, refill);
  }
}

ExternalSorter::~ExternalSorter() { Reset(); }

size_t ExternalSorter::EffectiveBudgetBytes() const noexcept {
  size_t b = options_.memory_budget_bytes;
  if (options_.grant != nullptr) {
    const size_t grant_op = options_.grant->OperatorBudgetBytes();
    if (b == 0 || grant_op < b) {
      b = grant_op;
    }
  }
  return b;
}

int ExternalSorter::CompareRows(const Row& lhs, const Row& rhs) const noexcept {
  for (size_t i = 0; i < key_orders_.size(); ++i) {
    const size_t col_idx = options_.keys[i].column_index;
    const bool lhs_ok = col_idx < lhs.Count();
    const bool rhs_ok = col_idx < rhs.Count();
    if (!lhs_ok || !rhs_ok) {
      if (lhs_ok == rhs_ok) {
        continue;
      }
      // A missing column is the NULL peer of a present one, so place it by
      // the key's NULLS FIRST/LAST rule instead of unconditionally sorting
      // "has column" after "missing column" (which can invert against a
      // NULL-valued cell).  Upstream normally supplies one schema, but a
      // spilled and a resident run can disagree after a projection change.
      const bool nulls_first = key_orders_[i].NullsFirst();
      return nulls_first ? (lhs_ok ? 1 : -1) : (lhs_ok ? -1 : 1);
    }
    const int cmp = key_orders_[i].Compare(lhs[col_idx], rhs[col_idx]);
    if (cmp != 0) {
      return cmp;
    }
  }
  return 0;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/PackedOrderKey.lean (sound)
// Lean proof: lean/TinylambOpt/ResearchProofs/StableRadix.lean (sound)
bool ExternalSorter::TrySortRowsPackedTwoKey(std::vector<Row>* rows) const {
  const char* disable_env = ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PACKED_ORDER_KEY");
  if (disable_env != nullptr && std::strcmp(disable_env, "1") == 0) {
    return false;
  }
  const size_t n = rows->size();
  if (n < 64 || n > std::numeric_limits<uint32_t>::max()) {
    return false;
  }
  if (options_.keys.size() < 2 || options_.keys.size() > 3 ||
      key_orders_.size() != options_.keys.size()) {
    return false;
  }
  const bool has_seq_col = (options_.keys.size() == 3);
  if (has_seq_col && !key_orders_[2].Ascending()) {
    return false;
  }

  const slot_t col0 = options_.keys[0].column_index;
  const slot_t col1 = options_.keys[1].column_index;
  const slot_t col2 = has_seq_col ? options_.keys[2].column_index : 0;
  const bool asc0 = key_orders_[0].Ascending();
  const bool nf0 = key_orders_[0].NullsFirst();
  const bool asc1 = key_orders_[1].Ascending();
  const bool nf1 = key_orders_[1].NullsFirst();

  struct PackedEntry {
    uint64_t key;
    uint32_t idx;
  };
  std::vector<PackedEntry> entries;
  std::vector<PackedEntry> scratch;
  try {
    entries.resize(n);
    scratch.resize(n);
  } catch (const std::bad_alloc&) {
    return false;
  }

  static constexpr int64_t kRangeLimit = int64_t{1} << 30;
  std::array<std::optional<ValueType>, 2> seen_type{};
  int64_t prev_seq = 0;

  auto encode_half = [&](const Row& row, slot_t col, size_t k_idx, bool asc,
                         bool nf, uint32_t* out) -> bool {
    if (col >= row.Count() || row[col].IsNull()) {
      *out = nf ? 0x00000000U : 0xFFFFFFFFU;
      return true;
    }
    const Value& v = row[col];
    if (v.type != ValueType::kDate &&
        (v.type != ValueType::kInt64 || v.IsUnsigned())) {
      return false;
    }
    if (!seen_type[k_idx].has_value()) {
      seen_type[k_idx] = v.type;
    } else if (*seen_type[k_idx] != v.type) {
      return false;
    }
    const int64_t iv = v.value.int_value;
    if (iv < -kRangeLimit || iv > kRangeLimit - 1) {
      return false;
    }
    auto u = static_cast<uint32_t>(iv + kRangeLimit);
    if (!asc) {
      u = 0x7FFFFFFFU - u;
    }
    *out = nf ? (0x80000000U | u) : u;
    return true;
  };

  for (size_t i = 0; i < n; ++i) {
    const Row& row = (*rows)[i];
    if (has_seq_col) {
      if (col2 >= row.Count() || row[col2].IsNull() ||
          row[col2].type != ValueType::kInt64 || row[col2].IsUnsigned()) {
        return false;
      }
      const int64_t seq = row[col2].value.int_value;
      if (i > 0 && seq <= prev_seq) {
        return false;
      }
      prev_seq = seq;
    }
    uint32_t h0 = 0;
    uint32_t h1 = 0;
    if (!encode_half(row, col0, 0, asc0, nf0, &h0) ||
        !encode_half(row, col1, 1, asc1, nf1, &h1)) {
      return false;
    }
    entries[i] = PackedEntry{
        .key = (static_cast<uint64_t>(h0) << 32) | static_cast<uint64_t>(h1),
        .idx = static_cast<uint32_t>(i)};
  }

  for (unsigned shift = 0; shift < 64; shift += 8) {
    std::array<uint32_t, 256> counts{};
    const auto first_byte =
        static_cast<uint8_t>((entries[0].key >> shift) & 0xFFU);
    bool all_same = true;
    for (const PackedEntry& e : entries) {
      const auto b = static_cast<uint8_t>((e.key >> shift) & 0xFFU);
      ++counts[b];
      if (b != first_byte) {
        all_same = false;
      }
    }
    if (all_same) {
      continue;
    }
    uint32_t offset = 0;
    std::array<uint32_t, 256> pos{};
    for (size_t b = 0; b < 256; ++b) {
      pos[b] = offset;
      offset += counts[b];
    }
    for (const PackedEntry& e : entries) {
      const auto b = static_cast<uint8_t>((e.key >> shift) & 0xFFU);
      scratch[pos[b]++] = e;
    }
    entries.swap(scratch);
  }

  for (size_t i = 0; i < n; ++i) {
    if (entries[i].idx == i) {
      continue;
    }
    Row tmp = std::move((*rows)[i]);
    size_t cur = i;
    while (entries[cur].idx != i) {
      const size_t nxt = entries[cur].idx;
      (*rows)[cur] = std::move((*rows)[nxt]);
      entries[cur].idx = static_cast<uint32_t>(cur);
      cur = nxt;
    }
    (*rows)[cur] = std::move(tmp);
    entries[cur].idx = static_cast<uint32_t>(cur);
  }
  return true;
}

// P34/61: identical to CompareRows but over the per-stream cached key cells
// (same Value objects were copied, same SortKeyOrder chain).
int ExternalSorter::CompareKeyCells(const MergeStream& lhs,
                                    const MergeStream& rhs) const noexcept {
  for (size_t i = 0; i < key_orders_.size(); ++i) {
    const bool lhs_ok = lhs.key_present[i] != 0;
    const bool rhs_ok = rhs.key_present[i] != 0;
    if (!lhs_ok || !rhs_ok) {
      if (lhs_ok == rhs_ok) {
        continue;
      }
      // Mirror CompareRows: a missing cache cell is the NULL peer of a
      // present one and follows the key's NULLS FIRST/LAST placement.
      const bool nulls_first = key_orders_[i].NullsFirst();
      return nulls_first ? (lhs_ok ? 1 : -1) : (lhs_ok ? -1 : 1);
    }
    const int cmp = key_orders_[i].Compare(lhs.key_cells[i], rhs.key_cells[i]);
    if (cmp != 0) {
      return cmp;
    }
  }
  return 0;
}

// P34/61: snapshot the stream's current row key columns into owned Values.
// resize/assign and the deep Value copy can allocate, so translate allocation
// failure into a Status instead of terminating inside this noexcept helper.
Status ExternalSorter::RefreshStreamKey(MergeStream* stream) noexcept {
  try {
    const size_t nk = key_orders_.size();
    // Build into locals and commit only on success: the loser tree compares
    // stream->key_cells directly, so a partially written cache would corrupt
    // ordering for every later comparison, not just this stream's.
    std::vector<Value> cells(nk);
    std::vector<uint8_t> present(nk, 0);
    if (!stream->eof && stream->row_idx < stream->block_rows.size()) {
      const Row& row = stream->block_rows[stream->row_idx];
      for (size_t i = 0; i < nk; ++i) {
        const size_t col_idx = options_.keys[i].column_index;
        if (col_idx < row.Count()) {
          present[i] = 1;
          cells[i] = row[col_idx];
        }
      }
    }
    stream->key_cells = std::move(cells);
    stream->key_present = std::move(present);
  } catch (const std::bad_alloc&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter key refresh out of memory");
  } catch (const std::length_error&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter key refresh too large");
  }
  return Status::kSuccess;
}

// Fast pre-check for the TopK heap. This duplicates SortKeyOrder::Compare
// (collation resolution, NaN/-0 canonicalisation, NULL placement) on purpose;
// any change to the canonical comparator must be mirrored here or rows may
// be wrongly skipped before reaching the heap.
bool ExternalSorter::ShouldSkipTopKChunkRow(
    const DataChunk& chunk, size_t row_idx,
    size_t sql_key_count) const noexcept {
  if (!in_limit_heap_ || spilled_ || run_rows_.empty() ||
      options_.force_spill ||
      ProcessMemoryBudget::Global().AlwaysSpillEnabled() ||
      sql_key_count == 0 || sql_key_count >= key_orders_.size() ||
      sql_key_count >= options_.keys.size() || row_idx >= chunk.Size()) {
    return false;
  }
  if (!key_orders_[sql_key_count].Ascending()) {
    return false;
  }
  // Every text key must reach AddRow, including later sort terms on rows
  // discarded by the first term.
  for (size_t i = 0; i < sql_key_count; ++i) {
    const size_t col = options_.keys[i].column_index;
    if (col < chunk.ColumnCount() &&
        (chunk.ColumnAt(col).Type() == ValueType::kVarChar ||
         chunk.ColumnAt(col).IsHeterogeneous())) { return false;
}
  }
  const Row& heap_root = run_rows_.front();
  for (size_t i = 0; i < sql_key_count; ++i) {
    const size_t col_idx = options_.keys[i].column_index;
    const bool lhs_ok = col_idx < chunk.ColumnCount();
    const bool rhs_ok = col_idx < heap_root.Count();
    if (!lhs_ok || !rhs_ok) {
      if (lhs_ok != rhs_ok) {
        // Mirror CompareRows/CompareKeyCells: a missing column is the NULL
        // peer of a present one and follows the key's NULLS FIRST/LAST
        // placement; a present-but-NULL value makes both sides NULL peers
        // -> tie. Unconditionally treating "has column" as sorting after
        // "missing column" can skip a row the real comparator would keep
        // under NULLS FIRST.
        const bool lhs_null =
            !lhs_ok || chunk.ColumnAt(col_idx).IsNull(row_idx);
        const bool rhs_null = !rhs_ok || heap_root[col_idx].IsNull();
        if (lhs_null == rhs_null) {
          continue;
        }
        const int cmp = key_orders_[i].NullsFirst() ? (lhs_null ? -1 : 1)
                                                    : (lhs_null ? 1 : -1);
        return cmp > 0;
      }
      continue;
    }
    const ColumnVector& col = chunk.ColumnAt(col_idx);
    const Value& rhs = heap_root[col_idx];
    const SortKeyOrder& order = key_orders_[i];
    const bool lhs_null = col.IsNull(row_idx);
    const bool rhs_null = rhs.IsNull();
    if (lhs_null || rhs_null) {
      if (lhs_null && rhs_null) {
        continue;
      }
      const int null_side = lhs_null ? -1 : 1;
      const int cmp = order.NullsFirst() ? null_side : -null_side;
      return cmp > 0;
    }
    int cmp = 0;
    if (col.IsHeterogeneous() || col.Type() != rhs.type) {
      cmp = order.Compare(col.ValueAt(row_idx), rhs);
    } else {
      switch (col.Type()) {
        case ValueType::kInt64: {
          if (col.IsUnsigned() || rhs.IsUnsigned()) {
            const bool mixed = col.IsUnsigned() != rhs.IsUnsigned();
            const int64_t lv = col.IntegerData()[row_idx];
            const int64_t rv = rhs.value.int_value;
            const auto ulv = static_cast<uint64_t>(lv);
            const auto urv = static_cast<uint64_t>(rv);
            int c = 0;
            if (mixed && !col.IsUnsigned() && lv < 0) {
              c = -1;
            } else if (mixed && !rhs.IsUnsigned() && rv < 0) {
              c = 1;
            } else {
              c = ulv < urv ? -1 : (urv < ulv ? 1 : 0);
            }
            cmp = order.Ascending() ? c : -c;
          } else {
            const int64_t lv = col.IntegerData()[row_idx];
            const int64_t rv = rhs.value.int_value;
            const int c = lv < rv ? -1 : (rv < lv ? 1 : 0);
            cmp = order.Ascending() ? c : -c;
          }
          break;
        }
        case ValueType::kDate: {
          const int64_t lv = col.IntegerData()[row_idx];
          const int64_t rv = rhs.value.int_value;
          const int c = lv < rv ? -1 : (rv < lv ? 1 : 0);
          cmp = order.Ascending() ? c : -c;
          break;
        }
        case ValueType::kDouble: {
          const double lv = col.DoubleData()[row_idx];
          const double rv = rhs.value.double_value;
          const bool nl = std::isnan(lv);
          const bool nr = std::isnan(rv);
          int c = 0;
          if (nl || nr) {
            c = (nl && nr) ? 0 : (nl ? -1 : 1);
          } else {
            c = lv < rv ? -1 : (rv < lv ? 1 : 0);
          }
          cmp = order.Ascending() ? c : -c;
          break;
        }
        case ValueType::kVarChar: {
          if (col.HasMixedCollation() || rhs.IsInterval()) {
            cmp = order.Compare(col.ValueAt(row_idx), rhs);
          } else {
            const Collation resolved = ResolveCollation(
                order.KeyCollation(),
                ResolveCollation(col.ColumnCollation(), CollationOf(rhs)));
            const std::string_view lv = col.StringData()[row_idx];
            const std::string_view rv = rhs.value.varchar_value;
            const int c = CompareVarCharForOrderBy(
                lv, rv, resolved == Collation::kCaseInsensitive);
            cmp = order.Ascending() ? c : -c;
          }
          break;
        }
        default:
          cmp = order.Compare(col.ValueAt(row_idx), rhs);
          break;
      }
    }
    if (cmp != 0) {
      return cmp > 0;
    }
  }
  return true;
}

void ExternalSorter::ObservePeakMemory(size_t bytes) noexcept {
  if (bytes > peak_mem_bytes_) {
    peak_mem_bytes_ = bytes;
    ProcessMemoryBudget::Global().RecordOperatorPeakBytes(bytes);
  }
}

void ExternalSorter::DropRunSegments() noexcept {
  for (const SpillSegmentId id : run_segments_) {
    SpillStore::DropSegmentIfAlive(options_.spill_store, spill_store_alive_,
                                   id);
  }
  run_segments_.clear();
}

void ExternalSorter::Reset() noexcept {
  for (size_t i = 0; i < key_orders_.size(); ++i) {
    key_orders_[i] = MakeSortKeyOrder(options_.keys[i]);
  }
  final_merge_.reset();
  final_streams_.clear();
  DropRunSegments();
  run_rows_.clear();
  // T3462: Reset is noexcept; shrink_to_fit's realloc could throw (and
  // terminate).  Swap with an empty vector instead.
  std::vector<Row>().swap(run_rows_);
  run_bytes_ = 0;
  in_limit_heap_ = false;
  in_mem_cursor_ = 0;
  total_rows_ = 0;
  emitted_rows_ = 0;
  spilled_bytes_ = 0;
  run_count_ = 0;
  merge_passes_ = 0;
  spilled_ = false;
  finished_ = false;
  if (grant_cache_.has_value()) {
    // Safe inside noexcept: ReleaseAll only calls the noexcept
    // QueryMemoryGrant::ReleaseConsumed (plus weak_ptr::lock) and does no
    // logging/allocating, so it cannot throw out of this teardown path.
    grant_cache_->ReleaseAll();
  }
}

Status ExternalSorter::WriteSortedRun(std::vector<Row>* rows,
                                      SpillSegmentId* seg_out) {
  if (options_.spill_store != nullptr && spill_store_alive_.expired()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "spill store is no longer alive");
  }
  if (options_.spill_store == nullptr) {
    return StatusError(
        StatusCode::kMemoryLimitExceeded,
        "ExternalSorter exceeded memory budget with no SpillStore");
  }
  try {
    if (!TrySortRowsPackedTwoKey(rows)) {
      std::stable_sort(
          rows->begin(), rows->end(),
          [this](const Row& a, const Row& b) { return RowPrecedes(a, b); });
    }
    if (options_.limit.has_value() && rows->size() > *options_.limit) {
      rows->resize(*options_.limit);
    }
  } catch (const std::bad_alloc&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter run sort out of memory");
  } catch (const std::length_error&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter run resize too large");
  }

  const size_t budget = EffectiveBudgetBytes();
  const size_t writer_buf =
      budget > 0 ? std::min<size_t>(SpillStore::kBlockSize,
                                    std::max<size_t>(4096, budget / 8))
                 : SpillStore::kBlockSize;
  SpillSegmentWriter writer(options_.spill_store, writer_buf);
  RETURN_IF_FAIL(writer.EnsureOpen());

  std::string scratch;
  size_t idx = 0;
  while (idx < rows->size()) {
    size_t batch_end = idx;
    // Reset per batch: leaving this outside the loop makes every row after
    // the first batch flush individually once the threshold is reached,
    // exhausting the temporary-space budget.
    size_t batch_bytes = 0;
    while (batch_end < rows->size() && (batch_end - idx) < kWriteBatchRows) {
      const size_t row_bytes = EstimateRowMemoryBytes((*rows)[batch_end]);
      // A wide row must still form its own batch so the merge can progress.
      if (batch_end != idx &&
          row_bytes > kWriteBatchMaxBytes -
                          std::min(batch_bytes, kWriteBatchMaxBytes)) {
        break;
      }
      batch_bytes += row_bytes;
      ++batch_end;
    }
    RETURN_IF_FAIL(WriteRowBlock(
        &writer, std::span<const Row>(rows->data() + idx, batch_end - idx),
        &scratch));
    idx = batch_end;
  }
  ASSIGN_OR_RETURN(SpillSegmentId, id, writer.Finish(true));
  spilled_bytes_ += writer.BytesWritten();
  *seg_out = id;
  return Status::kSuccess;
}

Status ExternalSorter::FlushCurrentRun() {
  if (run_rows_.empty()) {
    return Status::kSuccess;
  }
  if (in_limit_heap_) {
    in_limit_heap_ = false;
  }
  SpillSegmentId seg = kInvalidSpillSegmentId;
  RETURN_IF_FAIL(WriteSortedRun(&run_rows_, &seg));
  run_segments_.push_back(seg);
  ++run_count_;
  run_rows_.clear();
  run_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  spilled_ = true;
  return Status::kSuccess;
}

Status ExternalSorter::AddRow(const Row& row) {
  try {
    Row copy = row;
    return AddRow(std::move(copy));
  } catch (const std::bad_alloc&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter row copy out of memory");
  } catch (const std::length_error&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter row copy too large");
  }
}

Status ExternalSorter::AddRow(Row&& row) {
  if (finished_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot AddRow to finished ExternalSorter");
  }
  if (options_.spill_store != nullptr && spill_store_alive_.expired()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "spill store is no longer alive");
  }
  if (options_.limit.has_value() && *options_.limit == 0) {
    // Every submitted row is dropped, but still contributes to TotalRows().
    ++total_rows_;
    return Status::kSuccess;
  }
  for (size_t i = 0; i < key_orders_.size(); ++i) {
    const size_t col = options_.keys[i].column_index;
    if (col < row.Count()) {
      RETURN_IF_FAIL(key_orders_[i].Observe(row[col]));
    }
  }
  ++total_rows_;

  const size_t row_bytes = EstimateRowMemoryBytes(row);
  const size_t budget = EffectiveBudgetBytes();
  const bool always_spill = options_.force_spill ||
                            ProcessMemoryBudget::Global().AlwaysSpillEnabled();

  auto less_fn = [this](const Row& a, const Row& b) {
    return RowPrecedes(a, b);
  };

  // Bounded Top-N fast path: keep at most `*limit` rows in a max-heap when they
  // fit in budget and forced-spill is disabled.
  if (!spilled_ && !always_spill && options_.limit.has_value() &&
      *options_.limit > 0) {
    const size_t k = *options_.limit;
    if (!in_limit_heap_ && run_rows_.size() == k &&
        (budget == 0 || run_bytes_ <= budget)) {
      std::make_heap(run_rows_.begin(), run_rows_.end(), less_fn);
      in_limit_heap_ = true;
    }
    if (in_limit_heap_) {
      if (!RowPrecedes(row, run_rows_.front())) {
        return Status::kSuccess;  // Larger than current K-th smallest.
      }
      const size_t evicted_bytes = EstimateRowMemoryBytes(run_rows_.front());
      const size_t next_bytes = run_bytes_ > evicted_bytes
                                    ? run_bytes_ - evicted_bytes + row_bytes
                                    : row_bytes;
      bool can_replace_in_heap = (budget == 0 || next_bytes <= budget);
      if (can_replace_in_heap && grant_cache_.has_value() &&
          row_bytes > evicted_bytes) {
        if (!grant_cache_->Reserve(row_bytes - evicted_bytes)) {
          can_replace_in_heap = false;
        }
      } else if (can_replace_in_heap && grant_cache_.has_value() &&
                 evicted_bytes > row_bytes) {
        grant_cache_->Release(evicted_bytes - row_bytes);
      }
      if (can_replace_in_heap) {
        std::pop_heap(run_rows_.begin(), run_rows_.end(), less_fn);
        run_rows_.back() = std::move(row);
        std::push_heap(run_rows_.begin(), run_rows_.end(), less_fn);
        run_bytes_ = next_bytes;
        ObservePeakMemory(run_bytes_);
        return Status::kSuccess;
      }
      // Top-N payload outgrew memory budget; spill heap as run and fall
      // through.
      RETURN_IF_FAIL(FlushCurrentRun());
    }
  }

  bool row_grant_reserved = false;
  const bool need_flush =
      (always_spill && !run_rows_.empty() && run_bytes_ >= 16384) ||
      (budget > 0 && run_bytes_ + row_bytes > budget) ||
      (grant_cache_.has_value() &&
       !(row_grant_reserved = grant_cache_->Reserve(row_bytes)));

  if (need_flush) {
    RETURN_IF_FAIL(FlushCurrentRun());
    const bool row_exceeds_budget = budget > 0 && row_bytes > budget;
    if (!row_exceeds_budget && grant_cache_.has_value()) {
      row_grant_reserved = grant_cache_->Reserve(row_bytes);
    }
    if (row_exceeds_budget ||
        (grant_cache_.has_value() && !row_grant_reserved)) {
      if (options_.spill_store == nullptr) {
        return StatusError(
            StatusCode::kMemoryLimitExceeded,
            "ExternalSorter exceeded grant budget with no SpillStore "
            "configured");
      }
      ObservePeakMemory(row_bytes);
      std::vector<Row> single;
      try {
        single.push_back(std::move(row));
      } catch (const std::bad_alloc&) {
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "ExternalSorter single-row buffer out of memory");
      } catch (const std::length_error&) {
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "ExternalSorter single-row buffer too large");
      }
      SpillSegmentId seg = kInvalidSpillSegmentId;
      RETURN_IF_FAIL(WriteSortedRun(&single, &seg));
      try {
        run_segments_.push_back(seg);
      } catch (const std::bad_alloc&) {
        options_.spill_store->DropSegment(seg);
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "ExternalSorter run list out of memory");
      } catch (const std::length_error&) {
        options_.spill_store->DropSegment(seg);
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "ExternalSorter run list too large");
      }
      ++run_count_;
      spilled_ = true;
      return Status::kSuccess;
    }
  }

  try {
    run_rows_.push_back(std::move(row));
  } catch (const std::bad_alloc&) {
    if (row_grant_reserved) { grant_cache_->Release(row_bytes);
}
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter row buffer out of memory");
  } catch (const std::length_error&) {
    if (row_grant_reserved) { grant_cache_->Release(row_bytes);
}
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter row buffer too large");
  }
  run_bytes_ += row_bytes;
  ObservePeakMemory(run_bytes_);

  if (always_spill && run_bytes_ >= 16384) {
    RETURN_IF_FAIL(FlushCurrentRun());
  }
  return Status::kSuccess;
}

Status ExternalSorter::AddChunk(const DataChunk& chunk) {
  for (size_t r = 0; r < chunk.Size(); ++r) {
    RETURN_IF_FAIL(AddRow(chunk.RowAt(r)));
  }
  return Status::kSuccess;
}

Status ExternalSorter::AdvanceMergeStream(MergeStream* stream) {
  if (stream->eof) {
    return Status::kSuccess;
  }
  while (stream->row_idx >= stream->block_rows.size()) {
    // Free the previous block's high-water capacity as well as its rows:
    // keeping it would retain every stream's largest-ever 64 KiB block until
    // the merge ends, exceeding the computed budget (#12823). swap-with-empty
    // does not allocate, so it is safe on this Status path.
    std::vector<Row>().swap(stream->block_rows);
    stream->row_idx = 0;
    ASSIGN_OR_RETURN(bool, has_more,
                     ReadRowBlock(stream->reader.get(), &stream->block_rows,
                                  &stream->scratch));
    if (!has_more) {
      stream->eof = true;
      stream->scratch.clear();
      // T3462: shrink_to_fit can throw bad_alloc across the Status
      // boundary; swap-with-empty frees the capacity without allocating.
      std::string().swap(stream->scratch);
      return Status::kSuccess;
    }
  }
  return Status::kSuccess;
}

void ExternalSorter::ComputeMergeGeometry(
    size_t* merge_fan_in_out, size_t* reader_buf_out,
    size_t* writer_buf_out) const noexcept {
  const size_t budget = EffectiveBudgetBytes();
  if (budget == 0) {
    *reader_buf_out = SpillStore::kBlockSize;
    *writer_buf_out = SpillStore::kBlockSize;
    *merge_fan_in_out = 16;
    return;
  }
  const size_t rbuf =
      std::min<size_t>(16384, std::max<size_t>(4096, budget / 32));
  const size_t wbuf =
      std::min<size_t>(32768, std::max<size_t>(4096, budget / 16));
  const size_t usable_for_readers =
      budget > wbuf ? (budget - wbuf) : (budget / 2);
  // Each merge stream holds `rbuf` plus a decoded row block: a spill block is
  // up to SpillStore::kBlockSize (64 KiB), not the 16 KiB write-batch size.
  // Under-estimating this over-provisions fan-in and lets the merge exceed the
  // operator budget.
  const size_t per_stream = rbuf + SpillStore::kBlockSize;
  const size_t fan_in = std::clamp<size_t>(
      usable_for_readers / std::max<size_t>(per_stream, 8192), 2, 32);
  *reader_buf_out = rbuf;
  *writer_buf_out = wbuf;
  *merge_fan_in_out = fan_in;
}

Status ExternalSorter::MergeRunGroup(std::span<const SpillSegmentId> inputs,
                                     size_t reader_buf_bytes,
                                     size_t writer_buf_bytes,
                                     SpillSegmentId* merged_out) {
  if (options_.spill_store != nullptr && spill_store_alive_.expired()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "spill store is no longer alive");
  }
  try {
    std::vector<MergeStream> streams(inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i) {
      streams[i].reader = std::make_unique<SpillSegmentReader>(
          options_.spill_store, inputs[i], reader_buf_bytes);
      RETURN_IF_FAIL(AdvanceMergeStream(&streams[i]));
      RETURN_IF_FAIL(RefreshStreamKey(&streams[i]));
    }
    // P34/61: loser tree on cached key cells; equal keys break by stream
    // index (run order), keeping the merge deterministic.
    const auto key_beats = [this, &streams](int a, int b) {
      const int cmp = CompareKeyCells(streams[a], streams[b]);
      return cmp < 0 || (cmp == 0 && a < b);
    };
    std::vector<uint8_t> initial_active(inputs.size(), 1);
    for (size_t i = 0; i < inputs.size(); ++i) {
      initial_active[i] = streams[i].eof ? 0 : 1;
    }
    LoserMerge tree;
    tree.Build(inputs.size(), initial_active, key_beats);

    SpillSegmentWriter writer(options_.spill_store, writer_buf_bytes);
    RETURN_IF_FAIL(writer.EnsureOpen());

    std::vector<Row> batch;
    batch.reserve(kWriteBatchRows);
    size_t batch_bytes = 0;
    std::string write_scratch;
    size_t written = 0;

    while (tree.Top() >= 0) {
      if (options_.limit.has_value() && written >= *options_.limit) {
        // Top-N pruning: only the first `limit` rows of a merge can hold the
        // global top-N, so leaving the rest unread is intentional. The limit is
        // fixed in Options at construction (Reset() cannot clear it), so a
        // limit-less sorter never enters this branch and cannot lose runs.
        break;
      }
      const auto idx = static_cast<size_t>(tree.Top());

      Row row = std::move(streams[idx].block_rows[streams[idx].row_idx]);
      ++streams[idx].row_idx;
      batch_bytes += EstimateRowMemoryBytes(row);
      batch.push_back(std::move(row));
      ++written;

      if (batch.size() >= kWriteBatchRows ||
          batch_bytes >= kWriteBatchMaxBytes) {
        RETURN_IF_FAIL(WriteRowBlock(&writer, batch, &write_scratch));
        batch.clear();
        batch_bytes = 0;
      }

      RETURN_IF_FAIL(AdvanceMergeStream(&streams[idx]));
      RETURN_IF_FAIL(RefreshStreamKey(&streams[idx]));
      tree.SetActive(idx, !streams[idx].eof);
      tree.Replay(static_cast<int>(idx), key_beats);
    }

    if (!batch.empty()) {
      RETURN_IF_FAIL(WriteRowBlock(&writer, batch, &write_scratch));
      batch.clear();
    }
    ASSIGN_OR_RETURN(SpillSegmentId, out_id, writer.Finish(true));
    spilled_bytes_ += writer.BytesWritten();
    *merged_out = out_id;
    return Status::kSuccess;
  } catch (const std::bad_alloc&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter merge allocation out of memory");
  } catch (const std::length_error&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter merge allocation too large");
  }
}

Status ExternalSorter::OpenFinalMergeStreams(size_t reader_buf_bytes) {
  try {
    final_streams_.clear();
    final_merge_ = std::make_unique<LoserMerge>();
    final_streams_.resize(run_segments_.size());

    for (size_t i = 0; i < run_segments_.size(); ++i) {
      final_streams_[i].reader = std::make_unique<SpillSegmentReader>(
          options_.spill_store, run_segments_[i], reader_buf_bytes);
      RETURN_IF_FAIL(AdvanceMergeStream(&final_streams_[i]));
      RETURN_IF_FAIL(RefreshStreamKey(&final_streams_[i]));
    }
    const auto key_beats = [this](int a, int b) {
      const int cmp = CompareKeyCells(final_streams_[a], final_streams_[b]);
      return cmp < 0 || (cmp == 0 && a < b);
    };
    std::vector<uint8_t> initial_active(run_segments_.size(), 1);
    for (size_t i = 0; i < run_segments_.size(); ++i) {
      initial_active[i] = final_streams_[i].eof ? 0 : 1;
    }
    final_merge_->Build(run_segments_.size(), initial_active, key_beats);
    return Status::kSuccess;
  } catch (const std::bad_alloc&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter final merge allocation out of memory");
  } catch (const std::length_error&) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "ExternalSorter final merge allocation too large");
  }
}

Status ExternalSorter::Finish() {
  if (finished_) {
    return Status::kSuccess;
  }
  if (options_.spill_store != nullptr && spill_store_alive_.expired()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "spill store is no longer alive");
  }
  if (!spilled_) {
    if (in_limit_heap_) {
      auto less_fn = [this](const Row& a, const Row& b) {
        return RowPrecedes(a, b);
      };
      std::sort_heap(run_rows_.begin(), run_rows_.end(), less_fn);
      in_limit_heap_ = false;
    } else if (!TrySortRowsPackedTwoKey(&run_rows_)) {
      std::stable_sort(
          run_rows_.begin(), run_rows_.end(),
          [this](const Row& a, const Row& b) { return RowPrecedes(a, b); });
    }
    if (options_.limit.has_value() && run_rows_.size() > *options_.limit) {
      run_rows_.resize(*options_.limit);
    }
    in_mem_cursor_ = 0;
    emitted_rows_ = 0;
    finished_ = true;
    return Status::kSuccess;
  }

  if (!run_rows_.empty()) {
    RETURN_IF_FAIL(FlushCurrentRun());
  }

  size_t merge_fan_in = 16;
  size_t reader_buf = 16384;
  size_t writer_buf = 16384;
  ComputeMergeGeometry(&merge_fan_in, &reader_buf, &writer_buf);

  while (run_segments_.size() > merge_fan_in) {
    // Compact completed groups into the front of the ownership list. On
    // failure the remaining suffix still owns the original, unmerged runs.
    size_t completed = 0;
    for (size_t i = 0; i < run_segments_.size();) {
      const auto group =
          std::span<const SpillSegmentId>(run_segments_)
              .subspan(i, std::min(merge_fan_in, run_segments_.size() - i));
      SpillSegmentId output = group.front();
      if (group.size() > 1) {
        const Status merged =
            MergeRunGroup(group, reader_buf, writer_buf, &output);
        if (merged != Status::kSuccess) {
          run_segments_.erase(
              run_segments_.begin() + static_cast<ptrdiff_t>(completed),
              run_segments_.begin() + static_cast<ptrdiff_t>(i));
          return merged;
        }
        for (const auto input : group) {
          SpillStore::DropSegmentIfAlive(options_.spill_store,
                                         spill_store_alive_, input);
        }
      }
      i += group.size();
      run_segments_[completed++] = output;
    }
    run_segments_.resize(completed);
    ++merge_passes_;
  }

  if (!run_segments_.empty()) {
    // A lone remaining run is only wrapped in a cursor, not merged; counting
    // it here would inflate EXPLAIN's MergePasses by one. Count only after the
    // streams open successfully so a retried Finish() cannot double-count
    // (#12624).
    RETURN_IF_FAIL(OpenFinalMergeStreams(reader_buf));
    if (run_segments_.size() > 1) {
      ++merge_passes_;
    }
  }
  emitted_rows_ = 0;
  finished_ = true;
  return Status::kSuccess;
}

StatusOr<bool> ExternalSorter::NextRow(Row* row_out) {
  if (row_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null row_out in ExternalSorter::NextRow");
  }
  if (!finished_) {
    RETURN_IF_FAIL(Finish());
  }
  if (options_.limit.has_value() && emitted_rows_ >= *options_.limit) {
    final_streams_.clear();
    final_merge_.reset();
    DropRunSegments();  // no further rows will be read
    return false;
  }

  if (!spilled_) {
    if (in_mem_cursor_ >= run_rows_.size()) {
      return false;
    }
    *row_out = std::move(run_rows_[in_mem_cursor_++]);
    ++emitted_rows_;
    if (in_mem_cursor_ == run_rows_.size()) {
      run_rows_.clear();
      // T3462: shrink_to_fit can throw bad_alloc across this Status path.
      std::vector<Row>().swap(run_rows_);
      run_bytes_ = 0;
      if (grant_cache_.has_value()) {
        grant_cache_->ReleaseAll();
      }
    }
    return true;
  }

  if (final_merge_ == nullptr || final_merge_->Top() < 0) {
    final_streams_.clear();
    final_merge_.reset();
    DropRunSegments();
    return false;
  }

  const auto key_beats = [this](int a, int b) {
    const int cmp = CompareKeyCells(final_streams_[a], final_streams_[b]);
    return cmp < 0 || (cmp == 0 && a < b);
  };
  const auto idx = static_cast<size_t>(final_merge_->Top());
  MergeStream& stream = final_streams_[idx];

  // Pull the winner into a local first. The source advance (and its block
  // refill) happens before the pop is published to `row_out`/the loser tree,
  // so a failed read leaves the tree and cached key describing the row a
  // retry must emit again (#12636).
  Row emitted = std::move(stream.block_rows[stream.row_idx]);
  ++stream.row_idx;
  Status advance = AdvanceMergeStream(&stream);
  if (advance != Status::kSuccess) {
    // AdvanceMergeStream may already have cleared and partially refilled the
    // block; prepend the consumed row and point the cursor back at it. The
    // tree/key cells were left untouched, so the stream is consistent again.
    try {
      stream.block_rows.insert(stream.block_rows.begin(), std::move(emitted));
      stream.row_idx = 0;
    } catch (const std::bad_alloc&) {  // NOLINT(bugprone-empty-catch): keep the primary failure.
      // Best effort: even if the row cannot be restored, report the real
      // failure instead of leaking an exception across this Status path.
    }
    return advance;
  }
  // Refresh the loser key before publishing the emitted row. On failure the
  // tree still describes `emitted` (the key cache was left untouched), so
  // roll the stream cursor back and let a retry emit it again — the same
  // contract the advance-failure path above maintains.
  if (const Status key_status = RefreshStreamKey(&stream); !key_status.ok()) {
    try {
      stream.block_rows.insert(stream.block_rows.begin(), std::move(emitted));
      stream.row_idx = 0;
    } catch (const std::bad_alloc&) {  // NOLINT(bugprone-empty-catch): keep the primary failure.
      // Best effort, as above: report the real failure either way.
    }
    return key_status;
  }
  *row_out = std::move(emitted);
  ++emitted_rows_;

  final_merge_->SetActive(idx, !stream.eof);
  final_merge_->Replay(static_cast<int>(idx), key_beats);
  if (final_merge_->Top() < 0) {
    final_streams_.clear();
    final_merge_.reset();
    DropRunSegments();
  }
  return true;
}

StatusOr<bool> ExternalSorter::NextChunk(DataChunk* chunk_out,
                                         size_t max_rows) {
  if (chunk_out == nullptr || max_rows == 0) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid arguments in ExternalSorter::NextChunk");
  }
  chunk_out->Reset();
  Row row;
  while (chunk_out->Size() < max_rows) {
    ASSIGN_OR_RETURN(bool, has_row, NextRow(&row));
    if (!has_row) {
      break;
    }
    chunk_out->Append(std::move(row));
  }
  return !chunk_out->Empty();
}

}  // namespace tinylamb
