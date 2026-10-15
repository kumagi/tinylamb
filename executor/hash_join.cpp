/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "hash_join.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exc_shim.hpp"
#include "common/join_kind.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using PositionedRow = std::pair<Row, RowPosition>;

constexpr size_t kReactiveSpillPartitions = 16;
constexpr size_t kHybridPartitions = 32;
constexpr size_t kParallelBuildMinRows = 4096;
constexpr size_t kParallelProbeMinRows = size_t{1} << 15;

#if defined(__GNUC__) || defined(__clang__)
void PrefetchRow(const Row& row) {
  __builtin_prefetch(row.values_.data(), 0, 1);
}
#else
void PrefetchRow(const Row& row) { (void)row; }
#endif

void PrefetchProbeRow(const std::vector<PositionedRow>& rows, size_t index) {
  if (index < rows.size()) {
    PrefetchRow(rows[index].first);
  }
}

void PrefetchProbeRow(const std::vector<Row>& rows, size_t index) {
  if (index < rows.size()) {
    PrefetchRow(rows[index]);
  }
}

uint64_t BSwap64(uint64_t v) { return __builtin_bswap64(v); }

uint64_t MixHash(uint64_t h) {
  h *= 0x9E3779B97F4A7C15ULL;
  h ^= h >> 32;
  h *= 0xBF58476D1CE4E5B9ULL;
  h ^= h >> 29;
  return h;
}

uint64_t HashInt64Key(int64_t key) {
  return MixHash(static_cast<uint64_t>(key));
}

uint64_t HashBytesKey(std::string_view key) {
  uint64_t h = 14695981039346656037ULL;
  for (const char c : key) {
    h ^= static_cast<unsigned char>(c);
    h *= 1099511628211ULL;
  }
  return MixHash(h);
}

constexpr char kJoinNegativeIntTag = '\x7f';

bool ContainsUnmatchableNaN(const Value& value) {
  if (value.type == ValueType::kDouble) {
    return std::isnan(value.value.double_value);
  }
  if (value.type != ValueType::kArray) {
    return false;
  }
  return std::ranges::any_of(value.ArrayElements(), ContainsUnmatchableNaN);
}

void AppendMemComparableValue(const Value& v, std::string* out) {
  switch (v.type) {
    case ValueType::kInt64: {
      const bool negative = !v.IsUnsigned() && v.value.int_value < 0;
      out->push_back(negative ? kJoinNegativeIntTag
                              : static_cast<char>(ValueType::kInt64));
      const uint64_t be = BSwap64(static_cast<uint64_t>(v.value.int_value));
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kDate: {
      out->push_back(static_cast<char>(v.type));
      const uint64_t be = BSwap64(static_cast<uint64_t>(v.value.int_value));
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      buf[0] ^= static_cast<char>(0x80);
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kVarChar: {
      const std::string_view s(v.value.varchar_value.data(),
                               v.value.varchar_value.size());
      out->push_back(static_cast<char>(ValueType::kVarChar));
      if (s.empty()) {
        out->append(9, '\0');
        break;
      }
      for (size_t i = 0;; i += 8) {
        if (9 <= s.size() - i) {
          out->append(s.substr(i, 8));
          out->push_back('\x09');
        } else {
          const size_t tail = s.size() - i;
          out->append(s.substr(i, tail));
          out->append(8 - tail, '\0');
          out->push_back(
              static_cast<char>((s.size() % 8) + (s.size() % 8 == 0 ? 8 : 0)));
          break;
        }
      }
      break;
    }
    case ValueType::kDouble: {
      out->push_back(static_cast<char>(ValueType::kDouble));
      double d = v.value.double_value;
      if (std::isnan(d)) {
        d = std::numeric_limits<double>::quiet_NaN();
      } else if (d == 0.0) {
        d = 0.0;
      }
      uint64_t bits = 0;
      std::memcpy(&bits, &d, sizeof(bits));
      uint64_t be = BSwap64(bits);
      if (0 <= d) {
        be |= 0x80;
      } else {
        be = ~be;
      }
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kArray:
      out->append(v.EncodeMemcomparableFormat());
      break;
    case ValueType::kNull:
      CHECK_MSG(false, "Cannot encode unknown type.");
  }
}

// `null_safe` parallels `cols`: a true entry encodes a NULL key component as
// a distinct presence-marker byte (NULL IS NOT DISTINCT FROM NULL joins)
// instead of rejecting the row.  Rows are only comparable against encodings
// produced with the SAME null-safe layout, so callers must keep one flag per
// join (both sides) for the whole executor.
bool EncodeJoinKeyInto(const Row& row, const std::vector<slot_t>& cols,
                       std::string* out,
                       const std::vector<bool>* null_safe = nullptr) {
  out->clear();
  for (size_t i = 0; i < cols.size(); ++i) {
    const Value& value = row[cols[i]];
    const bool safe =
        null_safe != nullptr && i < null_safe->size() && (*null_safe)[i];
    if (value.IsNull()) {
      if (!safe) {
        return false;
      }
      out->push_back('\0');
      continue;
    }
    if (!safe && ContainsUnmatchableNaN(value)) {
      return false;
    }
    if (null_safe != nullptr) {
      out->push_back('\1');
    }
    if (safe && value.type == ValueType::kDouble &&
        std::isnan(value.value.double_value)) {
      out->append(Value(std::numeric_limits<double>::quiet_NaN())
                      .EncodeMemcomparableFormat());
    } else {
      AppendMemComparableValue(value, out);
    }
  }
  return true;
}

struct KeyRef {
  bool valid{false};
  int64_t int_key{0};
  std::string_view byte_key;
  uint64_t hash{0};
};

KeyRef KeyOf(const Row& row, const std::vector<slot_t>& cols,
             JoinHashIndex::KeyMode mode, ValueType int_mode_type,
             std::string* scratch,
             const std::vector<bool>* null_safe = nullptr) {
  if (mode == JoinHashIndex::KeyMode::kInt64) {
    const Value& v = row[cols[0]];
    if (v.IsNull() || v.type != int_mode_type) {
      return {};
    }
    return {.valid = true,
            .int_key = v.value.int_value,
            .byte_key = {},
            .hash = HashInt64Key(v.value.int_value)};
  }
  if (!EncodeJoinKeyInto(row, cols, scratch, null_safe)) {
    return {};
  }
  return {.valid = true,
          .int_key = 0,
          .byte_key = std::string_view(*scratch),
          .hash = HashBytesKey(*scratch)};
}

// The helper is intentionally a no-op for the row-only representation.
// NOLINTNEXTLINE(bugprone-return-const-ref-from-parameter)
const Row& RowOf(const Row& row) {
  // NOLINTNEXTLINE(bugprone-return-const-ref-from-parameter)
  return row;
}
const Row& RowOf(const PositionedRow& positioned) { return positioned.first; }

template <typename Container>
std::optional<ValueType> UniformIntLikeType(const Container& rows,
                                            const std::vector<slot_t>& cols) {
  if (cols.size() != 1) {
    return std::nullopt;
  }
  std::optional<ValueType> found;
  for (const auto& item : rows) {
    const Value& v = RowOf(item)[cols[0]];
    if (v.IsNull()) {
      continue;
    }
    if (v.IsUnsigned() ||
        (v.type != ValueType::kInt64 && v.type != ValueType::kDate) ||
        (found && *found != v.type)) {
      return std::nullopt;
    }
    found = v.type;
  }
  return found;
}

// Mode decision for the int64 fast path, shared by both join inputs.  The
// probe side always reuses the build side's mode, so deciding from the build
// rows alone lets a probe-side key slip into a domain it does not belong to:
// UINT64 values share the INT64 storage with an unsigned tag (raw bit
// patterns are NOT ordered like int64_t — UINT64_MAX reads as -1), and
// mixed-signedness equality is numeric (5u = 5 holds, -1 = 2^64-1 does not).
// Any unsigned key anywhere on either side therefore forces the byte
// encoding for the whole join.
template <typename L, typename R>
std::optional<ValueType> JoinIntModeType(
    const L& left_rows, const std::vector<slot_t>& left_cols,
    const R& right_rows, const std::vector<slot_t>& right_cols) {
  const std::optional<ValueType> right_mode =
      UniformIntLikeType(right_rows, right_cols);
  if (!right_mode.has_value()) {
    return std::nullopt;
  }
  const std::optional<ValueType> left_mode =
      UniformIntLikeType(left_rows, left_cols);
  if (left_mode != right_mode) {
    return std::nullopt;
  }
  return right_mode;
}

struct SideIndex {
  JoinHashIndex index;
  JoinHashIndex::KeyMode mode = JoinHashIndex::KeyMode::kBytes;
  ValueType int_type = ValueType::kInt64;
  bool has_null_key{false};
};

template <typename Container>
SideIndex BuildSideIndex(const Container& rows, const std::vector<slot_t>& cols,
                         std::optional<ValueType> int_mode,
                         const std::vector<bool>* null_safe = nullptr) {
  SideIndex side;
  // The int64 fast path cannot represent the null-presence marker, so a
  // null-safe key always uses the byte encoding.  `int_mode` is the joint
  // decision over both inputs (JoinIntModeType); per-partition rebuilds
  // reuse it so every partition of one join agrees on the encoding.
  if (null_safe == nullptr && int_mode.has_value()) {
    side.mode = JoinHashIndex::KeyMode::kInt64;
    side.int_type = *int_mode;
  }
  side.index.Init(side.mode, rows.size());
  std::string scratch;
  for (size_t i = 0; i < rows.size(); ++i) {
    for (const slot_t col : cols) {
      side.has_null_key = side.has_null_key || RowOf(rows[i])[col].IsNull();
    }
    const KeyRef k = KeyOf(RowOf(rows[i]), cols, side.mode, side.int_type,
                           &scratch, null_safe);
    if (k.valid) {
      side.index.Insert(k.hash, k.int_key, k.byte_key, i);
    }
  }
  return side;
}

template <typename Container>
size_t SumRowBytes(const Container& rows) {
  size_t total = 0;
  for (const auto& item : rows) {
    total += EstimateRowBytes(RowOf(item));
  }
  return total;
}

}  // namespace

void JoinHashIndex::Init(KeyMode mode, size_t expected_entries) {
  mode_ = mode;
  entries_.clear();
  arena_.clear();
  size_t slots = 16;
  while (slots * 7 < expected_entries * 10) {
    slots <<= 1;
  }
  slots_.assign(slots, kNil);
  if (mode_ == KeyMode::kInt64) {
    slot_int_keys_.assign(slots, 0);
    slot_byte_keys_.clear();
  } else {
    slot_int_keys_.clear();
    slot_byte_keys_.assign(slots, {0, 0});
  }
  mask_ = slots - 1;
  occupied_slots_ = 0;
  entries_.reserve(expected_entries);
}

uint64_t JoinHashIndex::HashInt64(int64_t key) { return HashInt64Key(key); }

uint64_t JoinHashIndex::HashBytes(std::string_view key) {
  return HashBytesKey(key);
}

void JoinHashIndex::StoreSlotKey(size_t slot, int64_t int_key,
                                 std::string_view byte_key) {
  if (mode_ == KeyMode::kInt64) {
    slot_int_keys_[slot] = int_key;
    return;
  }
  const auto off = static_cast<uint32_t>(arena_.size());
  arena_.append(byte_key);
  slot_byte_keys_[slot] = {off, static_cast<uint32_t>(byte_key.size())};
}

bool JoinHashIndex::SlotKeyEquals(size_t slot, int64_t int_key,
                                  std::string_view byte_key) const {
  if (mode_ == KeyMode::kInt64) {
    return slot_int_keys_[slot] == int_key;
  }
  const auto& span = slot_byte_keys_[slot];
  return std::string_view(arena_.data() + span.first, span.second) == byte_key;
}

void JoinHashIndex::Insert(uint64_t hash, int64_t int_key,
                           std::string_view byte_key, size_t row_index) {
  if (occupied_slots_ * 10 >= slots_.size() * 7) {
    Grow();
  }
  size_t idx = hash & mask_;
  for (;;) {
    const size_t head = slots_[idx];
    if (head == kNil) {
      StoreSlotKey(idx, int_key, byte_key);
      ++occupied_slots_;
      slots_[idx] = entries_.size();
      entries_.push_back(Entry{.row = row_index, .next = kNil});
      return;
    }
    if (SlotKeyEquals(idx, int_key, byte_key)) {
      entries_.push_back(Entry{.row = row_index, .next = slots_[idx]});
      slots_[idx] = entries_.size() - 1;
      return;
    }
    idx = (idx + 1) & mask_;
  }
}

size_t JoinHashIndex::Find(uint64_t hash, int64_t int_key,
                           std::string_view byte_key) const {
  size_t idx = hash & mask_;
  // Warm the next probe slot while the home slot is being fetched: linear
  // probing walks forward and int-key mode reads slot_int_keys_ right after
  // slots_, so two lines ahead is where the probe usually lands next.
  __builtin_prefetch(&slots_[(idx + 2) & mask_], 0, 1);
  for (;;) {
    const size_t head = slots_[idx];
    if (head == kNil) [[likely]] {
      return kNil;
    }
    if (SlotKeyEquals(idx, int_key, byte_key)) [[likely]] {
      return head;
    }
    idx = (idx + 1) & mask_;
  }
}

size_t JoinHashIndex::ChainNext(size_t entry) const {
  return entries_[entry].next;
}

size_t JoinHashIndex::RowIndex(size_t entry) const {
  return entries_[entry].row;
}

void JoinHashIndex::Grow() {
  const size_t new_count = slots_.size() * 2;
  const size_t new_mask = new_count - 1;
  std::vector<size_t> new_slots(new_count, kNil);
  std::vector<int64_t> new_int_keys;
  std::vector<std::pair<uint32_t, uint32_t>> new_byte_keys;
  if (mode_ == KeyMode::kInt64) {
    new_int_keys.resize(new_count);
  } else {
    new_byte_keys.resize(new_count);
  }
  for (size_t old = 0; old <= mask_; ++old) {
    if (slots_[old] == kNil) {
      continue;
    }
    const uint64_t hash = mode_ == KeyMode::kInt64
                              ? HashInt64Key(slot_int_keys_[old])
                              : HashBytesKey(std::string_view(
                                    arena_.data() + slot_byte_keys_[old].first,
                                    slot_byte_keys_[old].second));
    size_t idx = hash & new_mask;
    while (new_slots[idx] != kNil) {
      idx = (idx + 1) & new_mask;
    }
    if (mode_ == KeyMode::kInt64) {
      new_int_keys[idx] = slot_int_keys_[old];
    } else {
      new_byte_keys[idx] = slot_byte_keys_[old];
    }
    new_slots[idx] = slots_[old];
  }
  slots_ = std::move(new_slots);
  slot_int_keys_ = std::move(new_int_keys);
  slot_byte_keys_ = std::move(new_byte_keys);
  mask_ = new_mask;
}

struct HashJoin::JoinState {
  struct Side {
    std::vector<PositionedRow> rows;
    std::vector<SpillFile> spills;
    QueryMemoryCharge charge;
    Status child_status{Status::kSuccess};
    // Sticky SpillFile failure (EXC removal, migration Phase 7).
    Status spill_error{Status::kSuccess};
    bool has_null_key{false};
    // Rows the child produced, and how many of them this side's dynamic filter
    // rejected afterwards (0 when the scan applied the filter itself).
    size_t seen_rows{0};
    size_t filtered_rows{0};
    size_t filtered_null_rows{0};
    // Parallels the join's key columns (nullptr unless a null-safe key).
    const std::vector<bool>* null_safe{nullptr};
    // Scratch for the spill-partition key encoding, shared by Flush and
    // Accumulate so neither reallocates per row.
    std::string spill_key;
    // Comparison domain the dynamic filter was built over; a probe value of a
    // different ValueType is never rejected. `filter_rejects_all` covers the
    // domain-less RejectAll group (empty / all-NULL build side).
    ValueType filter_type{ValueType::kInt64};
    bool filter_rejects_all{false};

    [[nodiscard]] bool Spilled() const { return !spills.empty(); }

    // One row's worth of budget-aware accumulation: resident while the global
    // budget allows it, spilled into the matching hash partition otherwise.
    // Shared by the plain intake below and by the reduced semi/anti intake,
    // so both spill under exactly the same rules. Returns false once a spill
    // has failed, which stops the caller from pulling more rows from a child
    // whose output can no longer be stored.
    bool Accumulate(Row row, RowPosition position,
                    const std::vector<slot_t>& cols) {
      QueryMemoryBudget& budget = QueryMemoryBudget::Global();
      const size_t bytes = EstimateRowBytes(row);
      if (!Spilled() && !budget.CanReserve(bytes)) {
        Flush(cols);
      }
      if (Spilled()) {
        Status st_sp = Status::kSuccess;
        if (!EncodeJoinKeyInto(row, cols, &spill_key, null_safe)) {
          st_sp = spills[0].Append(row, position);
        } else {
          st_sp =
              spills[HashBytesKey(spill_key) % kReactiveSpillPartitions].Append(
                  row, position);
        }
        if (st_sp != Status::kSuccess) {
          spill_error = st_sp;
          return false;
        }
        return true;
      }
      charge.Add(bytes);
      rows.emplace_back(std::move(row), position);
      return true;
    }

    void Flush(const std::vector<slot_t>& cols) {
      spills.resize(kReactiveSpillPartitions);
      for (const PositionedRow& item : rows) {
        Status st_sp = Status::kSuccess;
        if (!EncodeJoinKeyInto(item.first, cols, &spill_key, null_safe)) {
          st_sp = spills[0].Append(item.first, item.second);
        } else {
          st_sp =
              spills[HashBytesKey(spill_key) % kReactiveSpillPartitions].Append(
                  item.first, item.second);
        }
        if (st_sp != Status::kSuccess) {
          spill_error = st_sp;
          break;
        }
      }
      charge.ReleaseAll();
      rows.clear();
      rows.shrink_to_fit();
    }

    // D8 (docs/design.md): strategies that cannot partition-join their input
    // (single-row subquery joins) reload every spilled partition instead of
    // silently dropping the spilled remainder.
    void LoadAll() {
      if (!Spilled()) {
        return;
      }
      std::vector<PositionedRow> loaded;
      for (SpillFile& file : spills) {
        if (file.Empty()) {
          continue;
        }
        StatusOr<std::vector<PositionedRow>> file_rows =
            file.ReadAllPositioned();
        if (!file_rows.HasValue()) {
          spill_error = file_rows.GetStatus();
          return;
        }
        for (auto&& item : file_rows.Value()) {
          loaded.push_back(std::move(item));
        }
      }
      spills.clear();
      for (const PositionedRow& item : loaded) {
        charge.Add(EstimateRowBytes(item.first));
      }
      rows = std::move(loaded);
    }
  };

  struct LinearKey {
    bool valid{false};
    bool is_null{false};
    uint64_t hash{0};
    std::string bytes;
  };

  // Fields are ordered to minimize padding (8-byte members first, flags last).
  const std::vector<PositionedRow>* build_rows = nullptr;
  const std::vector<slot_t>* build_cols = nullptr;
  const std::vector<slot_t>* probe_cols = nullptr;
  const std::vector<PositionedRow>* probe_rows = nullptr;
  std::vector<SpillFile>* probe_spills = nullptr;
  size_t probe_cursor = 0;
  size_t probe_index = 0;
  size_t spill_partition = 0;
  size_t spill_cursor = 0;
  size_t cur_shard = 0;
  size_t cur_entry = JoinHashIndex::kNil;
  size_t queue_index = 0;
  size_t queue_offset = 0;
  size_t nl_seen = 0;
  uint64_t nl_probe_hash = 0;
  size_t nl_pos = 0;

  std::vector<JoinHashIndex> shards;
  std::vector<PositionedRow> spill_cache;
  std::vector<std::vector<PositionedRow>> stripe_outputs;
  std::vector<std::vector<PositionedRow>> part_outputs;
  std::vector<LinearKey> linear_keys;
  std::string nl_probe_bytes;
  std::string scratch;

  Status spill_error{Status::kSuccess};
  Side left;
  Side right;

  uint32_t shard_bits = 0;
  JoinHashIndex::KeyMode key_mode = JoinHashIndex::KeyMode::kBytes;
  ValueType int_mode_type = ValueType::kInt64;
  bool left_builds = false;
  bool have_probe_row = false;
  // Adaptive small-build state: the probe runs as a nested loop over
  // `linear_keys` (the pre-encoded build keys) while the outer row count is
  // within HashJoin::kNestedLoopProbeLimit; beyond that the hash shards are
  // built and the remaining probe rows take the indexed path.
  bool defer_build = false;
  bool nl_row_active = false;
  bool nl_row_matched = false;
  bool nl_probe_valid = false;
  bool nl_probe_null = false;

  // `runtime_filter` is the dynamic join filter published for
  // `filter_column`. Scans apply it themselves before row decode; Consume
  // applies it only when the probe child could not, so a row is never tested
  // against the filter twice.
  static void Consume(ExecutorBase* child, const std::vector<slot_t>& cols,
                      Side* side,
                      const RuntimeFilterGroup* runtime_filter = nullptr,
                      std::optional<slot_t> filter_column = std::nullopt) {
    Row row;
    RowPosition position;
    std::atomic<size_t> filtered{0};
    std::atomic<size_t> filtered_null{0};
    const bool apply_filter =
        runtime_filter != nullptr && filter_column.has_value();
    while (child->Next(&row, &position)) {
      ++side->seen_rows;
      for (const slot_t col : cols) {
        side->has_null_key = side->has_null_key || row[col].IsNull();
      }
      if (apply_filter) {
        const Value& value = row[*filter_column];
        // A NULL join key never matches an equi-join, and a value outside the
        // filter's comparison domain is never rejected.
        if (value.IsNull() || side->filter_rejects_all ||
            (value.type == side->filter_type &&
             runtime_filter->Reject(value.value.int_value))) {
          ++filtered;
          if (value.IsNull()) {
            ++filtered_null;
          }
          continue;
        }
      }
      if (!side->Accumulate(std::move(row), position, cols)) {
        break;
      }
    }
    if (side->child_status == Status::kSuccess) {
      side->child_status = child->GetStatus();
    }
    if (filtered.load() != 0) {
      side->filtered_rows += filtered.load();
      side->filtered_null_rows += filtered_null.load();
    }
  }
};

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
                   std::vector<slot_t> right_cols, size_t worker_count)
    : HashJoin(std::move(left), std::move(left_cols), std::move(right),
               std::move(right_cols), HashJoinMode::kInMemory, worker_count) {}

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
                   std::vector<slot_t> right_cols, HashJoinMode mode,
                   size_t worker_count)
    : HashJoin(std::move(left), std::move(left_cols), std::move(right),
               std::move(right_cols), mode, JoinKind::kInner, worker_count) {}

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
                   std::vector<slot_t> right_cols, HashJoinMode mode,
                   JoinKind kind, size_t worker_count, size_t right_width,
                   size_t left_width)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)),
      mode_(mode),
      kind_(kind),
      right_width_(right_width),
      left_width_(left_width),
      worker_count_(std::max<size_t>(1, worker_count)) {}

HashJoin::~HashJoin() = default;

std::atomic<size_t> HashJoin::semi_reduction_max_domain_keys_{size_t{1} << 20};

void HashJoin::SetSemiReductionMaxDomainKeysForTest(size_t keys) {
  semi_reduction_max_domain_keys_.store(keys, std::memory_order_relaxed);
}

bool HashJoin::MaterializeOrThrow() {
  if (materialize_failed_) {
    return FailWith(materialize_error_);
  }
  const Status st = Materialize();
  if (st != Status::kSuccess) {
    materialize_failed_ = true;
    materialize_error_ = st;
    return FailWith(st);
  }
  return true;
}

bool HashJoin::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) {
    if (!MaterializeOrThrow()) {
      return false;
    }
  }
  if (state_ && state_->spill_error != Status::kSuccess) {
    return FailWith(state_->spill_error);
  }
  if (pipelined_) {
    return EmitNextMatch(dst, rp);
  }
  if (output_offset_ == output_.size()) {
    return false;
  }
  *dst = output_[output_offset_].first;
  if (rp != nullptr) {
    *rp = output_[output_offset_].second;
  }
  ++output_offset_;
  return true;
}

size_t HashJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (!materialized_) {
    if (!MaterializeOrThrow()) {
      return 0;
    }
  }
  if (state_ && state_->spill_error != Status::kSuccess) {
    FailWith(state_->spill_error);
    return 0;
  }
  Row row;
  RowPosition position;
  if (pipelined_) {
    while (destination->Size() < max_rows && EmitNextMatch(&row, &position)) {
      destination->Append(row, position);
    }
    return destination->Size();
  }
  while (output_offset_ < output_.size() && destination->Size() < max_rows) {
    destination->Append(output_[output_offset_].first,
                        output_[output_offset_].second);
    ++output_offset_;
  }
  return destination->Size();
}

Status HashJoin::Materialize() {
  output_.clear();
  output_offset_ = 0;
  if (kind_ == JoinKind::kLeftOuter || kind_ == JoinKind::kRightOuter ||
      kind_ == JoinKind::kFullOuter) {
    RETURN_IF_FAIL(MaterializeOuter());
    pipelined_ = false;
    materialized_ = true;
    return Status::kSuccess;
  }
  if (kind_ == JoinKind::kSingle) {
    RETURN_IF_FAIL(MaterializeSingle());
    pipelined_ = false;
    materialized_ = true;
    return Status::kSuccess;
  }
  if (kind_ == JoinKind::kMark) {
    RETURN_IF_FAIL(MaterializeMarkJoin());
    pipelined_ = false;
    materialized_ = true;
    return Status::kSuccess;
  }
  if (kind_ != JoinKind::kInner) {
    // Hash semi reduction: an existence-only join may shrink its hash build
    // (Phase 2 "Hash Semi Reduction"). Inner / outer / mark / single kinds are
    // not existence-only -- they emit or null-pad build columns -- so they keep
    // the unrestricted intake no matter what the plan asked for.
    const bool existence_only =
        semi_reduction_ &&
        (kind_ == JoinKind::kSemi || kind_ == JoinKind::kAnti ||
         kind_ == JoinKind::kNullAwareAnti);
    if (existence_only) {
      RETURN_IF_FAIL(MaterializeSemiAntiReduced());
    } else {
      RETURN_IF_FAIL(MaterializeSemiAnti());
    }
    pipelined_ = false;
    materialized_ = true;
    return Status::kSuccess;
  }
  if (mode_ == HashJoinMode::kHybrid) {
    RETURN_IF_FAIL(MaterializeHybrid());
    pipelined_ = false;
  } else {
    // Ignoring the status here left a half-built JoinState (probe_rows ==
    // nullptr) behind: the pipelined probe then dereferenced it and crashed
    // instead of surfacing the intake/spill error (sql_oracle_fuzz find).
    RETURN_IF_FAIL(MaterializeInMemory());
    pipelined_ = true;
  }
  materialized_ = true;

  return Status::kSuccess;
}

Status HashJoin::MaterializeSingle() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  RETURN_IF_FAIL(IntakeBothSides());
  // D8 (docs/design.md): the single-row (scalar subquery) join cannot stream
  // partition-by-partition, so reload any spilled input rather than dropping
  // the spilled rows and returning a wrong (missing) result.
  s.left.LoadAll();
  s.right.LoadAll();
  if (s.left.spill_error != Status::kSuccess) {
    return s.left.spill_error;
  }
  if (s.right.spill_error != Status::kSuccess) {
    return s.right.spill_error;
  }
  // Keep the intake/reload charges until JoinState dies: the rows stay
  // resident for the whole single-row join below, and releasing the charge
  // here would let the global budget be over-subscribed mid-join.

  const std::vector<PositionedRow>& left_rows = s.left.rows;
  const std::vector<PositionedRow>& right_rows = s.right.rows;
  std::string scratch;

  const std::optional<ValueType> int_mode =
      JoinIntModeType(left_rows, left_cols_, right_rows, right_cols_);
  const SideIndex build =
      BuildSideIndex(right_rows, right_cols_, int_mode, NullSafeArg());
  for (const auto& left : left_rows) {
    const KeyRef key = KeyOf(left.first, left_cols_, build.mode, build.int_type,
                             &scratch, NullSafeArg());
    const size_t first =
        key.valid ? build.index.Find(key.hash, key.int_key, key.byte_key)
                  : JoinHashIndex::kNil;
    if (first == JoinHashIndex::kNil) {
      output_.emplace_back(left.first + Row(std::vector<Value>(right_width_)),
                           left.second);
      continue;
    }
    size_t match_count = 0;
    size_t matched_right_index = 0;
    for (size_t entry = first; entry != JoinHashIndex::kNil;
         entry = build.index.ChainNext(entry)) {
      matched_right_index = build.index.RowIndex(entry);
      ++match_count;
      if (match_count > 1) {
        return StatusError(StatusCode::kInvalidArgument,
                           "SingleJoin subquery returned more than 1 row");
      }
    }
    output_.emplace_back(left.first + right_rows[matched_right_index].first,
                         left.second);
  }

  size_t output_bytes = 0;
  for (const auto& row : output_) {
    output_bytes += EstimateRowBytes(row.first);
  }
  output_charge_.Add(output_bytes);

  return Status::kSuccess;
}

Status HashJoin::MaterializeMarkJoin() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  RETURN_IF_FAIL(IntakeBothSides());
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();

  std::string scratch;
  size_t output_bytes = 0;
  const std::optional<ValueType> int_mode =
      JoinIntModeType(s.left.rows, left_cols_, s.right.rows, right_cols_);
  const auto emit_row = [&](const Row& row, const RowPosition& pos) {
    output_bytes += EstimateRowBytes(row);
    output_.emplace_back(row, pos);
  };

  const auto probe_lookup = [&](const PositionedRow& probe,
                                const SideIndex& build) -> int {
    const KeyRef k = KeyOf(probe.first, left_cols_, build.mode, build.int_type,
                           &scratch, NullSafeArg());
    if (!k.valid) {
      return -1;
    }
    return build.index.Find(k.hash, k.int_key, k.byte_key) !=
                   JoinHashIndex::kNil
               ? 1
               : 0;
  };

  size_t build_total = s.right.rows.size();
  for (const SpillFile& part : s.right.spills) {
    build_total += part.Count();
  }

  if (!s.right.Spilled()) {
    const SideIndex build =
        BuildSideIndex(s.right.rows, right_cols_, int_mode, NullSafeArg());
    const auto process_probe_rows =
        [&](const std::vector<PositionedRow>& rows) {
          for (const PositionedRow& probe : rows) {
            int match = probe_lookup(probe, build);
            Value marker;
            if (match == 1) {
              marker = Value(true);
            } else if (match == 0) {
              if (s.right.has_null_key) {
                marker = Value();
              } else {
                marker = Value(false);
              }
            } else {
              if (build_total == 0) {
                marker = Value(false);
              } else {
                marker = Value();
              }
            }
            emit_row(probe.first + Row({marker}), probe.second);
          }
        };

    if (!s.left.Spilled()) {
      process_probe_rows(s.left.rows);
    } else {
      for (SpillFile& part : s.left.spills) {
        ASSIGN_OR_RETURN(std::vector<PositionedRow>, h_read,
                         (part.ReadAllPositioned()));
        process_probe_rows(h_read);
      }
    }
    output_charge_.Add(output_bytes);
    return Status::kSuccess;
  }

  const auto process_probe_spilled =
      [&](const std::vector<PositionedRow>& rows) -> Status {
    std::vector<int> match_state(rows.size(), 0);
    for (size_t i = 0; i < rows.size(); ++i) {
      std::string scratch_key;
      const KeyRef k =
          KeyOf(rows[i].first, left_cols_, JoinHashIndex::KeyMode::kBytes,
                ValueType::kInt64, &scratch_key, NullSafeArg());
      if (!k.valid) {
        match_state[i] = -1;
      }
    }

    for (size_t p = 0; p < kReactiveSpillPartitions; ++p) {
      ASSIGN_OR_RETURN(std::vector<PositionedRow>, right_part,
                       (s.right.spills[p].ReadAllPositioned()));
      if (right_part.empty()) {
        continue;
      }
      const SideIndex build =
          BuildSideIndex(right_part, right_cols_, int_mode, NullSafeArg());
      for (size_t i = 0; i < rows.size(); ++i) {
        if (match_state[i] == 1 || match_state[i] == -1) {
          continue;
        }
        if (probe_lookup(rows[i], build) == 1) {
          match_state[i] = 1;
        }
      }
    }

    for (size_t i = 0; i < rows.size(); ++i) {
      Value marker;
      if (match_state[i] == 1) {
        marker = Value(true);
      } else if (match_state[i] == 0) {
        if (s.right.has_null_key) {
          marker = Value();
        } else {
          marker = Value(false);
        }
      } else {
        if (build_total == 0) {
          marker = Value(false);
        } else {
          marker = Value();
        }
      }
      emit_row(rows[i].first + Row({marker}), rows[i].second);
    }
    return Status::kSuccess;
  };

  if (!s.left.Spilled()) {
    RETURN_IF_FAIL(process_probe_spilled(s.left.rows));
  } else {
    for (SpillFile& part : s.left.spills) {
      ASSIGN_OR_RETURN(std::vector<PositionedRow>, h_read,
                       (part.ReadAllPositioned()));
      RETURN_IF_FAIL(process_probe_spilled(h_read));
    }
  }
  output_charge_.Add(output_bytes);

  return Status::kSuccess;
}

Status HashJoin::MaterializeOuter() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  RETURN_IF_FAIL(IntakeBothSides());
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();

  std::vector<PositionedRow> left_rows;
  left_rows.reserve(s.left.rows.size());
  for (const PositionedRow& row : s.left.rows) {
    left_rows.push_back(row);
  }
  for (SpillFile& part : s.left.spills) {
    ASSIGN_OR_RETURN(std::vector<PositionedRow>, rows,
                     (part.ReadAllPositioned()));
    left_rows.insert(left_rows.end(), rows.begin(), rows.end());
  }
  std::vector<PositionedRow> right_rows;
  right_rows.reserve(s.right.rows.size());
  for (const PositionedRow& row : s.right.rows) {
    right_rows.push_back(row);
  }
  for (SpillFile& part : s.right.spills) {
    ASSIGN_OR_RETURN(std::vector<PositionedRow>, rows,
                     (part.ReadAllPositioned()));
    right_rows.insert(right_rows.end(), rows.begin(), rows.end());
  }
  if (right_width_ == 0 && !right_rows.empty()) {
    right_width_ = right_rows.front().first.values_.size();
  }
  if (left_width_ == 0 && !left_rows.empty()) {
    left_width_ = left_rows.front().first.values_.size();
  }
  // D8 (docs/design.md): outer joins reprocess resident + spilled rows
  // together (no silent row loss); the reload is charged to the query budget
  // so RSS growth stays observable instead of vanishing from accounting.
  for (const PositionedRow& row : left_rows) {
    s.left.charge.Add(EstimateRowBytes(row.first));
  }
  for (const PositionedRow& row : right_rows) {
    s.right.charge.Add(EstimateRowBytes(row.first));
  }

  const bool preserve_left = kind_ != JoinKind::kRightOuter;
  const bool preserve_right = kind_ != JoinKind::kLeftOuter;
  std::vector<bool> matched_right(right_rows.size(), false);
  std::string scratch;
  const std::optional<ValueType> int_mode =
      JoinIntModeType(left_rows, left_cols_, right_rows, right_cols_);
  const auto pair_passes = [&](const PositionedRow& left,
                               const PositionedRow& right) -> StatusOr<bool> {
    if (!residual_predicate_) {
      return true;
    }
    StatusOr<Value> value = residual_predicate_->TryEvaluate(
        left.first + right.first, residual_schema_);
    if (!value.HasValue()) {
      return value.GetStatus();
    }
    return !value.Value().IsNull() && value.Value().Truthy();
  };

  if (preserve_left) {
    const SideIndex build =
        BuildSideIndex(right_rows, right_cols_, int_mode, NullSafeArg());
    for (const auto& left : left_rows) {
      const KeyRef key = KeyOf(left.first, left_cols_, build.mode,
                               build.int_type, &scratch, NullSafeArg());
      const size_t first =
          key.valid ? build.index.Find(key.hash, key.int_key, key.byte_key)
                    : JoinHashIndex::kNil;
      bool left_matched = false;
      if (first == JoinHashIndex::kNil) {
        output_.emplace_back(left.first + Row(std::vector<Value>(right_width_)),
                             left.second);
        continue;
      }
      for (size_t entry = first; entry != JoinHashIndex::kNil;
           entry = build.index.ChainNext(entry)) {
        const size_t right_index = build.index.RowIndex(entry);
        StatusOr<bool> passes = pair_passes(left, right_rows[right_index]);
        if (!passes.HasValue()) {
          return passes.GetStatus();
        }
        if (!passes.Value()) {
          continue;
        }
        left_matched = true;
        matched_right[right_index] = true;
        output_.emplace_back(left.first + right_rows[right_index].first,
                             left.second);
      }
      if (!left_matched) {
        output_.emplace_back(left.first + Row(std::vector<Value>(right_width_)),
                             left.second);
      }
    }
  } else {
    const SideIndex build =
        BuildSideIndex(left_rows, left_cols_, int_mode, NullSafeArg());
    for (size_t right_index = 0; right_index < right_rows.size();
         ++right_index) {
      const PositionedRow& right = right_rows[right_index];
      const KeyRef key = KeyOf(right.first, right_cols_, build.mode,
                               build.int_type, &scratch, NullSafeArg());
      const size_t first =
          key.valid ? build.index.Find(key.hash, key.int_key, key.byte_key)
                    : JoinHashIndex::kNil;
      if (first == JoinHashIndex::kNil) {
        continue;
      }
      for (size_t entry = first; entry != JoinHashIndex::kNil;
           entry = build.index.ChainNext(entry)) {
        const PositionedRow& left = left_rows[build.index.RowIndex(entry)];
        StatusOr<bool> passes = pair_passes(left, right);
        if (!passes.HasValue()) {
          return passes.GetStatus();
        }
        if (!passes.Value()) {
          continue;
        }
        matched_right[right_index] = true;
        output_.emplace_back(left.first + right.first, left.second);
      }
    }
  }
  if (preserve_right) {
    for (size_t right_index = 0; right_index < right_rows.size();
         ++right_index) {
      if (matched_right[right_index]) {
        continue;
      }
      output_.emplace_back(
          Row(std::vector<Value>(left_width_)) + right_rows[right_index].first,
          right_rows[right_index].second);
    }
  }
  size_t output_bytes = 0;
  for (const auto& row : output_) {
    output_bytes += EstimateRowBytes(row.first);
  }
  output_charge_.Add(output_bytes);

  return Status::kSuccess;
}

Status HashJoin::MaterializeSemiAnti() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  RETURN_IF_FAIL(IntakeBothSides());
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();
  SemiAntiBuild build;
  build.total_rows = s.right.rows.size();
  for (const SpillFile& part : s.right.spills) {
    build.total_rows += part.Count();
  }
  build.has_null_key = s.right.has_null_key;
  // The reduced intake replaces the unrestricted one above only for
  // existence-only kinds; everything else keeps the unchanged pipeline.
  return EmitSemiAnti(
      JoinIntModeType(s.left.rows, left_cols_, s.right.rows, right_cols_),
      build);
}

Status HashJoin::MaterializeSemiAntiReduced() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  s.left.null_safe = NullSafeArg();
  s.right.null_safe = NullSafeArg();

  // 1) The probe side first. It is the domain the build side is filtered
  //    against, and it must be COMPLETE before a single build row is judged, so
  //    it is consumed exactly as the plain pipeline consumes it -- resident
  //    rows plus every spilled partition, which together are the whole child
  //    output. This is not an extra pass over the input: the plain pipeline
  //    materializes the probe side before the first lookup as well.
  JoinState::Consume(left_.get(), left_cols_, &s.left);
  if (s.left.child_status != Status::kSuccess) {
    return s.left.child_status;
  }
  if (s.left.spill_error != Status::kSuccess) {
    return s.left.spill_error;
  }
  s.left.charge.ReleaseAll();

  // 2) The probe key domain. Built from the same memcomparable byte encoding
  //    (and the same per-key null-safety annotation) the build index uses, so
  //    a membership test here decides exactly what the real lookup would. A
  //    NULL key encodes to the null-rejecting failure and therefore adds no
  //    domain member, matching the probe phase, where such a row matches
  //    nothing either.
  std::unique_ptr<JoinHashIndex> domain = std::make_unique<JoinHashIndex>();
  domain->Init(JoinHashIndex::KeyMode::kBytes, 0);
  std::string domain_key;
  size_t domain_keys = 0;
  bool domain_capped = false;
  const auto observe = [&](const PositionedRow& probe) {
    if (domain_capped) {
      return;
    }
    if (!EncodeJoinKeyInto(probe.first, left_cols_, &domain_key,
                           NullSafeArg())) {
      return;
    }
    const uint64_t hash = HashBytesKey(domain_key);
    if (domain->Find(hash, 0, domain_key) != JoinHashIndex::kNil) {
      return;
    }
    if (domain_keys >=
        semi_reduction_max_domain_keys_.load(std::memory_order_relaxed)) {
      domain_capped = true;
      return;
    }
    // The stored row index is the domain ordinal, which doubles as the "a build
    // row with this key was already kept" bit below.
    domain->Insert(hash, 0, domain_key, domain_keys);
    ++domain_keys;
  };
  for (const PositionedRow& probe : s.left.rows) {
    observe(probe);
  }
  if (!domain_capped) {
    for (SpillFile& part : s.left.spills) {
      ASSIGN_OR_RETURN(std::vector<PositionedRow>, part_rows,
                       (part.ReadAllPositioned()));
      for (const PositionedRow& probe : part_rows) {
        observe(probe);
      }
    }
  }
  if (domain_capped) {
    // The probe side is itself a huge key domain: holding it would cost more
    // than the build rows it could prune, so the domain is released and the
    // intake below runs unrestricted. Deduplication stays, because it is
    // unconditionally sound for an existence-only join. The emitted rows are
    // identical either way.
    domain.reset();
    semi_reduction_description_ = "fallback=domain_cap";
  } else {
    semi_reduction_probe_keys_ = domain_keys;
    semi_reduction_description_ =
        domain_keys == 0 ? "empty_probe_domain" : "probe_key_domain";
  }

  // 3) The build side, through the domain. `kept` marks the domain ordinals
  //    that already have a resident build row, so a duplicate key is dropped
  //    instead of extending an entry chain the existence lookup never walks.
  //    On the fallback path a second index plays the same role, over the keys
  //    that were actually kept.
  std::vector<bool> kept(domain_keys, false);
  std::unique_ptr<JoinHashIndex> seen;
  if (domain == nullptr) {
    seen = std::make_unique<JoinHashIndex>();
    seen->Init(JoinHashIndex::KeyMode::kBytes, 0);
  }
  size_t seen_keys = 0;
  bool raw_build_null_key = false;
  std::string build_key;
  Row build_row;
  RowPosition build_position;
  while (right_->Next(&build_row, &build_position)) {
    ++semi_reduction_build_rows_;
    // Mirrors JoinState::Consume's own accounting: `NOT IN` is UNKNOWN for
    // every non-match as soon as the build side carries a NULL key, and an
    // empty build side makes `x NOT IN S` true for every x. Both read the RAW
    // build side, not the reduced one.
    for (const slot_t col : right_cols_) {
      raw_build_null_key = raw_build_null_key || build_row[col].IsNull();
    }
    if (!EncodeJoinKeyInto(build_row, right_cols_, &build_key, NullSafeArg())) {
      // No encodable key: the row can never join anything.
      continue;
    }
    const uint64_t hash = HashBytesKey(build_key);
    if (domain != nullptr) {
      const size_t entry = domain->Find(hash, 0, build_key);
      if (entry == JoinHashIndex::kNil) {
        // No probe row carries this key, so this build row joins nothing.
        continue;
      }
      const size_t ordinal = domain->RowIndex(entry);
      if (kept[ordinal]) {
        ++semi_reduction_duplicate_keys_;
        continue;
      }
      kept[ordinal] = true;
    } else {
      const size_t entry = seen->Find(hash, 0, build_key);
      if (entry != JoinHashIndex::kNil) {
        ++semi_reduction_duplicate_keys_;
        continue;
      }
      seen->Insert(hash, 0, build_key, seen_keys);
      ++seen_keys;
    }
    if (!s.right.Accumulate(std::move(build_row), build_position,
                            right_cols_)) {
      break;
    }
    ++semi_reduction_build_rows_kept_;
  }
  if (s.right.spill_error != Status::kSuccess) {
    return s.right.spill_error;
  }
  if (right_->GetStatus() != Status::kSuccess) {
    return right_->GetStatus();
  }
  s.right.charge.ReleaseAll();
  semi_reduction_active_ = true;

  SemiAntiBuild build;
  build.total_rows = semi_reduction_build_rows_;
  build.has_null_key = raw_build_null_key;
  return EmitSemiAnti(
      JoinIntModeType(s.left.rows, left_cols_, s.right.rows, right_cols_),
      build);
}

Status HashJoin::EmitSemiAnti(std::optional<ValueType> int_mode,
                              const SemiAntiBuild& build) {
  JoinState& s = *state_;
  const bool semi = kind_ == JoinKind::kSemi;
  std::string scratch;
  size_t output_bytes = 0;
  const auto emit_probe = [&](const PositionedRow& probe) {
    output_bytes += EstimateRowBytes(probe.first);
    output_.emplace_back(probe.first, probe.second);
  };
  // Existence lookup of one probe row against an already-built index.
  // Returns: 0 = no match, 1 = match, -1 = NULL key (never matches).
  const auto probe_lookup = [&](const PositionedRow& probe,
                                const SideIndex& side) -> int {
    const KeyRef k = KeyOf(probe.first, left_cols_, side.mode, side.int_type,
                           &scratch, NullSafeArg());
    if (!k.valid) {
      return -1;
    }
    return side.index.Find(k.hash, k.int_key, k.byte_key) != JoinHashIndex::kNil
               ? 1
               : 0;
  };
  // A NULL probe key can never match, so kAnti (NOT EXISTS decorrelation)
  // must emit it exactly like a no-match row: nested-loop and merge joins
  // already do.  kNullAwareAnti (NOT IN) keeps three-valued semantics: a
  // NULL probe key is UNKNOWN and filtered out.
  const bool null_aware = kind_ == JoinKind::kNullAwareAnti;
  const auto probe_survives = [&](int state) {
    if (semi) {
      return state == 1;
    }
    if (null_aware) {
      return state == 0;
    }
    return state != 1;
  };

  if (null_aware && build.has_null_key) {
    // NOT IN is UNKNOWN for every non-matching probe when the build set has a
    // NULL key; UNKNOWN is filtered out by a surrounding WHERE.
    return Status::kSuccess;
  }

  // Raw build-side cardinality decides how NULL probe keys behave: an empty
  // set makes `x NOT IN S` true for every x (anti emits everything), while a
  // non-empty set turns it into UNKNOWN unless x is found (NULL probes are
  // then dropped). Callers decorrelating NOT IN must guarantee a NOT NULL
  // build key so this case cannot arise (see optimizer.cpp).
  const size_t build_total = build.total_rows;
  if (!semi && build_total == 0) {
    if (!s.left.Spilled()) {
      for (const PositionedRow& probe : s.left.rows) {
        emit_probe(probe);
      }
    } else {
      for (SpillFile& part : s.left.spills) {
        ASSIGN_OR_RETURN(std::vector<PositionedRow>, h_read,
                         (part.ReadAllPositioned()));
        for (const PositionedRow& probe : h_read) {
          emit_probe(probe);
        }
      }
    }
    output_charge_.Add(output_bytes);
    return Status::kSuccess;
  }

  if (!s.right.Spilled()) {
    // Resident build side: index it once and stream every left partition
    // through an existence check.
    const SideIndex side_index =
        BuildSideIndex(s.right.rows, right_cols_, int_mode, NullSafeArg());
    const auto stream_left = [&](const std::vector<PositionedRow>& rows) {
      for (const PositionedRow& probe : rows) {
        if (probe_survives(probe_lookup(probe, side_index))) {
          emit_probe(probe);
        }
      }
    };
    if (!s.left.Spilled()) {
      stream_left(s.left.rows);
    } else {
      for (SpillFile& part : s.left.spills) {
        ASSIGN_OR_RETURN(std::vector<PositionedRow>, h_read,
                         (part.ReadAllPositioned()));
        stream_left(h_read);
      }
    }
    output_charge_.Add(output_bytes);
    return Status::kSuccess;
  }

  // Build side spilled: both sides partition by the same key hash, so a
  // per-partition index is exhaustive.
  if (!s.left.Spilled()) {
    // Resident probe side: one row may match in any partition, so match
    // states accumulate across partitions before emission.  Three states
    // (PRODUCTION FIX): -1 = NULL probe key, 0 = no match, 1 = match.  The
    // old bool array turned a NULL probe key into "no match", which emitted
    // NULL-key rows for ANTI joins although `NULL NOT IN (...)` is UNKNOWN.
    enum class ProbeState : int8_t { kNull = -1, kNoMatch = 0, kMatch = 1 };
    std::vector<ProbeState> matched(s.left.rows.size(), ProbeState::kNoMatch);
    for (size_t p = 0; p < kReactiveSpillPartitions; ++p) {
      ASSIGN_OR_RETURN(std::vector<PositionedRow>, right_part,
                       (s.right.spills[p].ReadAllPositioned()));
      if (right_part.empty()) {
        continue;
      }
      const SideIndex side_index =
          BuildSideIndex(right_part, right_cols_, int_mode, NullSafeArg());
      for (size_t i = 0; i < s.left.rows.size(); ++i) {
        if (matched[i] == ProbeState::kMatch) {
          continue;
        }
        const int state = probe_lookup(s.left.rows[i], side_index);
        if (state == 1) {
          matched[i] = ProbeState::kMatch;
        } else if (state < 0 && matched[i] == ProbeState::kNoMatch) {
          matched[i] = ProbeState::kNull;
        }
      }
    }
    for (size_t i = 0; i < s.left.rows.size(); ++i) {
      // Semi emits only matches; anti emits both kNoMatch and kNull because
      // a NULL probe key matches nothing.
      if (probe_survives(static_cast<int>(matched[i]))) {
        emit_probe(s.left.rows[i]);
      }
    }
  } else {
    for (size_t p = 0; p < kReactiveSpillPartitions; ++p) {
      ASSIGN_OR_RETURN(std::vector<PositionedRow>, right_part,
                       (s.right.spills[p].ReadAllPositioned()));
      ASSIGN_OR_RETURN(std::vector<PositionedRow>, left_part,
                       (s.left.spills[p].ReadAllPositioned()));
      if (left_part.empty()) {
        continue;
      }
      if (right_part.empty()) {
        if (!semi) {
          for (const PositionedRow& probe : left_part) {
            emit_probe(probe);
          }
        }
        continue;
      }
      const SideIndex side_index =
          BuildSideIndex(right_part, right_cols_, int_mode, NullSafeArg());
      for (const PositionedRow& probe : left_part) {
        if (probe_survives(probe_lookup(probe, side_index))) {
          emit_probe(probe);
        }
      }
    }
  }
  output_charge_.Add(output_bytes);

  return Status::kSuccess;
}

Status HashJoin::IntakeBothSides() {
  JoinState& s = *state_;
  s.left.null_safe = NullSafeArg();
  s.right.null_safe = NullSafeArg();
  std::jthread left_thread(
      [&] { JoinState::Consume(left_.get(), left_cols_, &s.left); });
  JoinState::Consume(right_.get(), right_cols_, &s.right);
  left_thread.join();
  if (s.left.child_status != Status::kSuccess) {
    return s.left.child_status;
  }
  if (s.right.child_status != Status::kSuccess) {
    return s.right.child_status;
  }
  if (s.left.spill_error != Status::kSuccess) {
    return s.left.spill_error;
  }
  if (s.right.spill_error != Status::kSuccess) {
    return s.right.spill_error;
  }
  return Status::kSuccess;
}

void HashJoin::BuildShards() {
  JoinState& s = *state_;
  const std::vector<PositionedRow>& rows = *s.build_rows;
  CHECK_MSG(s.build_cols != nullptr, "hash join build side is not configured");
  const std::vector<slot_t>& cols = *s.build_cols;
  if (auto uniform =
          NullSafeArg() == nullptr && s.probe_rows != nullptr &&
                  s.probe_cols != nullptr
              ? JoinIntModeType(*s.probe_rows, *s.probe_cols, rows, cols)
              : std::optional<ValueType>();
      uniform.has_value()) {
    s.key_mode = JoinHashIndex::KeyMode::kInt64;
    s.int_mode_type = *uniform;
  } else {
    s.key_mode = JoinHashIndex::KeyMode::kBytes;
  }

  const bool parallel_build =
      worker_count_ > 1 && rows.size() >= kParallelBuildMinRows;
  uint32_t bits = 0;
  if (parallel_build) {
    const size_t want = std::min(worker_count_, size_t{8});
    while ((size_t{1} << bits) < want) {
      ++bits;
    }
  }
  s.shard_bits = bits;
  const size_t shard_count = size_t{1} << bits;

  const auto shard_of = [bits](uint64_t hash) -> size_t {
    return bits == 0 ? 0 : static_cast<size_t>(hash >> (64 - bits));
  };

  std::vector<uint64_t> hashes(rows.size());
  std::vector<size_t> counts(shard_count + 1, 0);
  std::string scratch;
  for (size_t i = 0; i < rows.size(); ++i) {
    const KeyRef k = KeyOf(rows[i].first, cols, s.key_mode, s.int_mode_type,
                           &scratch, NullSafeArg());
    hashes[i] = k.hash;
    ++counts[shard_of(hashes[i]) + 1];
  }
  for (size_t i = 0; i < shard_count; ++i) {
    counts[i + 1] += counts[i];
  }
  std::vector<size_t> sizes(shard_count, 0);
  for (size_t i = 0; i < rows.size(); ++i) {
    ++sizes[shard_of(hashes[i])];
  }
  std::vector<size_t> ordered(rows.size());
  {
    std::vector<size_t> cursor(counts.begin(), counts.end() - 1);
    for (size_t i = 0; i < rows.size(); ++i) {
      ordered[cursor[shard_of(hashes[i])]++] = i;
    }
  }

  s.shards.resize(shard_count);
  for (size_t k = 0; k < shard_count; ++k) {
    s.shards[k].Init(s.key_mode, sizes[k]);
  }

  const auto fill_range = [&](size_t begin, size_t end) {
    std::string local_scratch;
    for (size_t pos = begin; pos < end; ++pos) {
      const size_t i = ordered[pos];
      const KeyRef k = KeyOf(rows[i].first, cols, s.key_mode, s.int_mode_type,
                             &local_scratch, NullSafeArg());
      // NULL (invalid) build keys must stay out of the index: their default
      // KeyRef carries int_key=0/hash=0, which a probe key of 0 would chain
      // onto as a false inner join (BuildSideIndex enforces the same
      // contract).
      if (!k.valid) {
        continue;
      }
      s.shards[shard_of(k.hash)].Insert(k.hash, k.int_key, k.byte_key, i);
    }
  };

  if (!parallel_build || shard_count == 1) {
    fill_range(0, ordered.size());
    return;
  }
  std::exception_ptr error;
  std::mutex error_mutex;
  std::vector<std::jthread> threads;
  threads.reserve(shard_count);
  for (size_t k = 0; k < shard_count; ++k) {
    const size_t begin = counts[k];
    const size_t end = k + 1 < shard_count ? counts[k + 1] : ordered.size();
    try {
      threads.emplace_back([&, begin, end] {
        try {
          fill_range(begin, end);
        } catch (...) {
          std::scoped_lock lock(error_mutex);
          if (!error) {
            error = std::current_exception();
          }
        }
      });
    } catch (...) {
      std::scoped_lock lock(error_mutex);
      if (!error) {
        error = std::current_exception();
      }
      break;
    }
  }
  threads.clear();
  if (error) {
    std::rethrow_exception(error);
  }
}

uint32_t HashJoin::ShardOf(uint64_t hash) const {
  if (state_->shard_bits == 0) {
    return 0;
  }
  return static_cast<uint32_t>(hash >> (64 - state_->shard_bits));
}

bool HashJoin::FetchNextProbe() {
  JoinState& s = *state_;
  if (s.probe_spills != nullptr) {
    while (s.spill_cursor >= s.spill_cache.size()) {
      if (s.spill_partition >= s.probe_spills->size()) {
        s.have_probe_row = false;
        return false;
      }
      StatusOr<std::vector<PositionedRow>> next_part =
          (*s.probe_spills)[s.spill_partition++].ReadAllPositioned();
      if (!next_part.HasValue()) {
        s.spill_error = next_part.GetStatus();
        s.have_probe_row = false;
        return false;
      }
      s.spill_cache = next_part.MoveValue();
      s.spill_cursor = 0;
    }
    s.probe_index = s.spill_cursor++;
    s.have_probe_row = true;
    return true;
  }
  if (s.probe_cursor >= s.probe_rows->size()) {
    s.have_probe_row = false;
    return false;
  }
  s.probe_index = s.probe_cursor++;
  s.have_probe_row = true;
  return true;
}

void HashJoin::SetupInMemoryJoin() {
  JoinState& s = *state_;
  // The dynamic filter path already chose the orientation (it had to, to know
  // which side to publish from); keep it instead of re-deciding from byte
  // sizes, which would point the filter at the wrong side.
  s.left_builds = dynamic_filter_forced_build_left_
                      ? dynamic_filter_build_left_
                      : SumRowBytes(s.left.rows) < SumRowBytes(s.right.rows);
  build_left_side_ = s.left_builds;
  if (s.left_builds) {
    s.build_rows = &s.left.rows;
    s.build_cols = &left_cols_;
    s.probe_rows = &s.right.rows;
    s.probe_cols = &right_cols_;
  } else {
    s.build_rows = &s.right.rows;
    s.build_cols = &right_cols_;
    s.probe_rows = &s.left.rows;
    s.probe_cols = &left_cols_;
  }
  memory_peak_bytes_ = SumRowBytes(*s.build_rows) + SumRowBytes(*s.probe_rows);
  spill_partition_count_ = (s.left.Spilled() ? kReactiveSpillPartitions : 0) +
                           (s.right.Spilled() ? kReactiveSpillPartitions : 0);

  // Small builds install an exact-set runtime filter and start the probe as
  // a nested loop over the build side (no hash build at all while the outer
  // stays tiny); crossing the probe threshold switches to the hash index.
  const size_t build_count = s.build_rows->size();
  runtime_filter_active_ = kind_ == JoinKind::kInner && !s.left.Spilled() &&
                           !s.right.Spilled() && build_count > 0 &&
                           build_count <= kSmallBuildThreshold;
  runtime_filter_keys_ = runtime_filter_active_ ? build_count : 0;
  s.defer_build =
      runtime_filter_active_ && s.probe_rows->size() < kParallelProbeMinRows;
  if (s.defer_build) {
    s.linear_keys.reserve(build_count);
    for (const PositionedRow& item : *s.build_rows) {
      JoinState::LinearKey key;
      key.valid = EncodeJoinKeyInto(item.first, *s.build_cols, &key.bytes,
                                    NullSafeArg());
      key.is_null = std::ranges::any_of(
          *s.build_cols, [&](slot_t col) { return item.first[col].IsNull(); });
      if (key.valid) {
        key.hash = HashBytesKey(key.bytes);
      }
      s.linear_keys.push_back(std::move(key));
    }
  } else {
    BuildShards();
  }
  actual_build_rows_ = s.build_rows == nullptr ? 0 : s.build_rows->size();
  actual_probe_rows_ = s.probe_rows == nullptr ? 0 : s.probe_rows->size();
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();
  if (!s.defer_build && worker_count_ > 1 &&
      s.probe_rows->size() >= kParallelProbeMinRows) {
    RunStripedProbe();
  }
}

void HashJoin::SetupOneSideSpilled() {
  JoinState& s = *state_;
  s.left_builds = !s.left.Spilled();
  build_left_side_ = s.left_builds;
  if (s.left_builds) {
    s.build_rows = &s.left.rows;
    s.build_cols = &left_cols_;
    BuildShards();
    s.probe_spills = &s.right.spills;
    s.probe_cols = &right_cols_;
  } else {
    s.build_rows = &s.right.rows;
    s.build_cols = &right_cols_;
    BuildShards();
    s.probe_spills = &s.left.spills;
    s.probe_cols = &left_cols_;
  }
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();
}

template <typename RightCont>
void HashJoin::JoinPartitionPair(const std::vector<PositionedRow>& left_part,
                                 const RightCont& right_part,
                                 std::vector<PositionedRow>* out) {
  if (left_part.empty() || right_part.empty()) {
    return;
  }
  const bool left_builds = SumRowBytes(left_part) < SumRowBytes(right_part);
  // Partitioned paths re-derive the key domain per pair; the byte encoding
  // is domain-safe for any input, so no int fast path here.
  const SideIndex build =
      left_builds
          ? BuildSideIndex(left_part, left_cols_, std::nullopt, NullSafeArg())
          : BuildSideIndex(right_part, right_cols_, std::nullopt,
                           NullSafeArg());
  const std::vector<slot_t>& probe_cols =
      left_builds ? right_cols_ : left_cols_;
  std::string scratch;
  if (left_builds) {
    for (size_t probe_index = 0; probe_index < right_part.size();
         ++probe_index) {
      PrefetchProbeRow(right_part, probe_index + 8);
      const auto& probe_row = right_part[probe_index];
      const KeyRef k = KeyOf(RowOf(probe_row), probe_cols, build.mode,
                             build.int_type, &scratch, NullSafeArg());
      if (!k.valid) {
        continue;
      }
      for (size_t e = build.index.Find(k.hash, k.int_key, k.byte_key);
           e != JoinHashIndex::kNil; e = build.index.ChainNext(e)) {
        const PositionedRow& b = left_part[build.index.RowIndex(e)];
        out->emplace_back(b.first + RowOf(probe_row), b.second);
      }
    }
  } else {
    for (size_t probe_index = 0; probe_index < left_part.size();
         ++probe_index) {
      PrefetchProbeRow(left_part, probe_index + 8);
      const PositionedRow& probe = left_part[probe_index];
      const KeyRef k = KeyOf(probe.first, probe_cols, build.mode,
                             build.int_type, &scratch, NullSafeArg());
      if (!k.valid) {
        continue;
      }
      for (size_t e = build.index.Find(k.hash, k.int_key, k.byte_key);
           e != JoinHashIndex::kNil; e = build.index.ChainNext(e)) {
        out->emplace_back(
            probe.first + RowOf(right_part[build.index.RowIndex(e)]),
            probe.second);
      }
    }
  }
}

Status HashJoin::SetupBothSpilled() {
  JoinState& s = *state_;
  s.part_outputs.assign(kReactiveSpillPartitions, {});
  const size_t workers = std::min(worker_count_, kReactiveSpillPartitions);
  std::atomic<size_t> next_partition{0};
  Status error{Status::kSuccess};
  std::mutex error_mutex;
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  for (size_t worker = 0; worker < workers; ++worker) {
    try {
      threads.emplace_back([&] {
        try {
          for (;;) {
            const size_t p = next_partition.fetch_add(1);
            if (p >= kReactiveSpillPartitions) {
              break;
            }
            StatusOr<std::vector<PositionedRow>> left_part =
                s.left.spills[p].ReadAllPositioned();
            StatusOr<std::vector<PositionedRow>> right_part =
                s.right.spills[p].ReadAllPositioned();
            if (!left_part.HasValue() || !right_part.HasValue()) {
              std::scoped_lock lock(error_mutex);
              if (error == Status::kSuccess) {
                error = left_part.HasValue() ? right_part.GetStatus()
                                             : left_part.GetStatus();
              }
              return;
            }
            JoinPartitionPair(left_part.Value(), right_part.Value(),
                              &s.part_outputs[p]);
          }
        } catch (const std::exception& ex) {
          std::scoped_lock lock(error_mutex);
          if (error == Status::kSuccess) {
            error = StatusError(StatusCode::kRuntimeError, ex.what());
          }
        } catch (...) {
          std::scoped_lock lock(error_mutex);
          if (error == Status::kSuccess) {
            error = StatusError(StatusCode::kRuntimeError,
                                "parallel spill worker failed");
          }
        }
      });
    } catch (const std::exception& ex) {
      std::scoped_lock lock(error_mutex);
      error = StatusError(StatusCode::kRuntimeError, ex.what());
      break;
    } catch (...) {
      std::scoped_lock lock(error_mutex);
      error = StatusError(StatusCode::kRuntimeError,
                          "failed to start parallel spill worker");
      break;
    }
  }
  threads.clear();
  if (error != Status::kSuccess) {
    return error;
  }
  size_t output_bytes = 0;
  for (const auto& part : s.part_outputs) {
    for (const auto& item : part) {
      output_bytes += EstimateRowBytes(item.first);
    }
  }
  output_charge_.Add(output_bytes);
  return Status::kSuccess;
}

void HashJoin::RunStripedProbe() {
  JoinState& s = *state_;
  const size_t n = s.probe_rows->size();
  const size_t workers = std::min(worker_count_, n);
  const size_t chunk = (n + workers - 1) / workers;
  std::vector<std::vector<PositionedRow>> outs(workers);
  std::exception_ptr error;
  std::mutex error_mutex;
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  for (size_t worker = 0; worker < workers; ++worker) {
    const size_t begin = worker * chunk;
    const size_t end = std::min(n, begin + chunk);
    if (begin >= end) {
      break;
    }
    try {
      threads.emplace_back([&, worker, begin, end] {
        try {
          std::string scratch;
          for (size_t i = begin; i < end; ++i) {
            PrefetchProbeRow(*s.probe_rows, i + 8);
            const PositionedRow& probe = (*s.probe_rows)[i];
            const KeyRef k = KeyOf(probe.first, *s.probe_cols, s.key_mode,
                                   s.int_mode_type, &scratch, NullSafeArg());
            if (!k.valid) {
              continue;
            }
            const size_t shard =
                s.shard_bits == 0 ? 0 : k.hash >> (64 - s.shard_bits);
            const JoinHashIndex& table = s.shards[shard];
            for (size_t e = table.Find(k.hash, k.int_key, k.byte_key);
                 e != JoinHashIndex::kNil; e = table.ChainNext(e)) {
              const PositionedRow& b = (*s.build_rows)[table.RowIndex(e)];
              if (s.left_builds) {
                outs[worker].emplace_back(b.first + probe.first, b.second);
              } else {
                outs[worker].emplace_back(probe.first + b.first, probe.second);
              }
            }
          }
        } catch (...) {
          std::scoped_lock lock(error_mutex);
          if (!error) {
            error = std::current_exception();
          }
        }
      });
    } catch (...) {
      std::scoped_lock lock(error_mutex);
      if (!error) {
        error = std::current_exception();
      }
      break;
    }
  }
  threads.clear();
  if (error) {
    std::rethrow_exception(error);
  }
  size_t output_bytes = 0;
  for (const auto& stripe : outs) {
    for (const auto& item : stripe) {
      output_bytes += EstimateRowBytes(item.first);
    }
  }
  join_matches_ = 0;
  for (const auto& stripe : outs) {
    join_matches_ += stripe.size();
  }
  output_charge_.Add(output_bytes);
  s.stripe_outputs = std::move(outs);
}

Status HashJoin::MaterializeInMemory() {
  state_ = std::make_unique<JoinState>();
  if (DynamicFilterEligible()) {
    RETURN_IF_FAIL(MaterializeDynamicFilter());
  } else {
    RETURN_IF_FAIL(IntakeBothSides());
  }
  return MaterializeInMemoryTail();
}

Status HashJoin::MaterializeInMemoryTail() {
  JoinState& s = *state_;
  const bool left_spilled = s.left.Spilled();
  const bool right_spilled = s.right.Spilled();
  if (left_spilled && right_spilled) {
    RETURN_IF_FAIL(SetupBothSpilled());
  } else if (left_spilled || right_spilled) {
    SetupOneSideSpilled();
  } else {
    SetupInMemoryJoin();
  }

  return Status::kSuccess;
}

bool HashJoin::DynamicFilterEligible() const {
  if (!dynamic_filter_ || kind_ != JoinKind::kInner || pipelined_ ||
      state_ == nullptr || materialized_) {
    return false;
  }
  // A null-safe (IS NOT DISTINCT FROM) key matches NULL to NULL, so a filter
  // that rejects NULL probe keys would drop real matches. The build side's
  // NULL key domain would also have to be part of the filter.
  if (NullSafeArg() != nullptr) {
    return false;
  }
  if (mode_ == HashJoinMode::kHybrid) {
    return false;
  }
  // The runtime filter is defined over a single INT64/DATE key; composite and
  // non-integer equijoins keep the unchanged path.
  const std::vector<slot_t>& build_cols =
      dynamic_filter_build_left_ ? left_cols_ : right_cols_;
  const std::vector<slot_t>& probe_cols =
      dynamic_filter_build_left_ ? right_cols_ : left_cols_;
  return build_cols.size() == 1 && probe_cols.size() == 1;
}

Status HashJoin::MaterializeDynamicFilter() {
  JoinState& s = *state_;
  s.left.null_safe = nullptr;
  s.right.null_safe = nullptr;
  const bool build_left = dynamic_filter_build_left_;
  JoinState::Side& build = build_left ? s.left : s.right;
  JoinState::Side& probe = build_left ? s.right : s.left;
  Executor& build_child = build_left ? left_ : right_;
  Executor& probe_child = build_left ? right_ : left_;
  const std::vector<slot_t>& build_cols = build_left ? left_cols_ : right_cols_;
  const std::vector<slot_t>& probe_cols = build_left ? right_cols_ : left_cols_;

  // 1) Drain the build side first: the filter must describe its COMPLETE key
  //    domain before a single probe row is tested against it.
  std::jthread build_thread(
      [&] { JoinState::Consume(build_child.get(), build_cols, &build); });
  build_thread.join();
  if (build.child_status != Status::kSuccess) {
    return build.child_status;
  }
  if (build.spill_error != Status::kSuccess) {
    return build.spill_error;
  }

  // 2) Derive the filter. A spilled build side is an incomplete key domain, so
  //    no filter is published for it (the join still runs, just unpruned).
  std::shared_ptr<const RuntimeFilterGroup> group;
  ValueType key_type = ValueType::kNull;
  if (!build.Spilled()) {
    RuntimeFilterBuilder builder;
    for (const PositionedRow& item : build.rows) {
      const Value& key = item.first[build_cols[0]];
      if (key.IsNull()) {
        builder.ObserveNull();
        continue;
      }
      if (key_type == ValueType::kNull) {
        key_type = key.type;
      } else if (key_type != key.type) {
        // A build side mixing INT64 and DATE keys has no single int64
        // comparison domain; leave the probe side unfiltered.
        key_type = ValueType::kNull;
        break;
      }
      builder.Observe(key.value.int_value);
    }
    // allow_reject_all: an empty or all-NULL build side makes this inner
    // equi-join empty, and a RejectAll filter lets the probe scan skip
    // everything instead of decoding rows that cannot match. Such a group has
    // no comparison domain, so it is publishable even when no build key ever
    // revealed the column's type.
    group = builder.Build(/*allow_reject_all=*/true);
    if (group != nullptr && !group->RejectsAll() &&
        key_type != ValueType::kInt64 && key_type != ValueType::kDate) {
      group.reset();
    }
  }
  if (group != nullptr && !group->empty()) {
    probe.filter_type = key_type;
    probe.filter_rejects_all = group->RejectsAll();
    const bool pushed =
        probe_child->AttachRuntimeFilter(group, probe_cols[0], key_type);
    if (pushed) {
      dynamic_filter_published_ = true;
    }
    // When the probe child cannot prune, Consume applies the filter instead.
    dynamic_filter_keys_ = group->DistinctKeys();
    dynamic_filter_description_ = group->Describe();
  } else {
    group.reset();
  }

  // 3) Drain the probe side, through the scan's own filter when it took one.
  if (dynamic_filter_published_) {
    JoinState::Consume(probe_child.get(), probe_cols, &probe);
  } else {
    JoinState::Consume(
        probe_child.get(), probe_cols, &probe, group.get(),
        group != nullptr ? std::optional<slot_t>(probe_cols[0]) : std::nullopt);
  }
  dynamic_filter_forced_build_left_ = build_left;
  dynamic_filter_probe_rows_ = probe.seen_rows;
  dynamic_filter_rejected_ = probe.filtered_rows;
  dynamic_filter_null_rejected_ = probe.filtered_null_rows;
  if (probe.child_status != Status::kSuccess) {
    return probe.child_status;
  }
  if (probe.spill_error != Status::kSuccess) {
    return probe.spill_error;
  }
  return Status::kSuccess;
}

bool HashJoin::EmitNextMatch(Row* dst, RowPosition* rp) {
  JoinState& s = *state_;
  if (!s.stripe_outputs.empty() || !s.part_outputs.empty()) {
    auto& queues = s.stripe_outputs.empty() ? s.part_outputs : s.stripe_outputs;
    while (s.queue_index < queues.size()) {
      auto& queue = queues[s.queue_index];
      if (s.queue_offset < queue.size()) {
        PositionedRow& item = queue[s.queue_offset++];
        *dst = std::move(item.first);
        if (rp != nullptr) {
          *rp = item.second;
        }
        return true;
      }
      ++s.queue_index;
      s.queue_offset = 0;
    }
    return false;
  }

  while (true) {
    // Nested-loop phase: stream the current probe row's linear matches over
    // the pre-encoded build keys (small build, outer still below the switch
    // threshold).
    if (s.nl_row_active) {
      while (s.nl_pos < s.linear_keys.size()) {
        const JoinState::LinearKey& b = s.linear_keys[s.nl_pos];
        const size_t build_index = s.nl_pos;
        ++s.nl_pos;
        if (s.nl_probe_valid && b.valid && b.hash == s.nl_probe_hash &&
            b.bytes == s.nl_probe_bytes) {
          const PositionedRow& probe = (*s.probe_rows)[s.probe_index];
          const PositionedRow& build_row = (*s.build_rows)[build_index];
          s.nl_row_matched = true;
          if (s.left_builds) {
            *dst = build_row.first + probe.first;
            if (rp != nullptr) {
              *rp = build_row.second;
            }
          } else {
            *dst = probe.first + build_row.first;
            if (rp != nullptr) {
              *rp = probe.second;
            }
          }
          ++join_matches_;
          return true;
        }
      }
      if (runtime_filter_active_ && !s.nl_row_matched) {
        probe_rows_rejected_.fetch_add(1);
        if (s.nl_probe_null) {
          probe_rows_null_rejected_.fetch_add(1);
        }
      }
      s.nl_row_active = false;
      s.have_probe_row = false;
      continue;
    }
    if (s.have_probe_row && s.cur_entry != JoinHashIndex::kNil) {
      const PositionedRow& probe = s.probe_spills != nullptr
                                       ? s.spill_cache[s.probe_index]
                                       : (*s.probe_rows)[s.probe_index];
      const PositionedRow& b =
          (*s.build_rows)[s.shards[s.cur_shard].RowIndex(s.cur_entry)];
      if (s.left_builds) {
        *dst = b.first + probe.first;
        if (rp != nullptr) {
          *rp = b.second;
        }
      } else {
        *dst = probe.first + b.first;
        if (rp != nullptr) {
          *rp = probe.second;
        }
      }
      s.cur_entry = s.shards[s.cur_shard].ChainNext(s.cur_entry);
      ++join_matches_;
      return true;
    }
    if (!FetchNextProbe()) {
      if (s.defer_build) {
        // The outer never crossed the threshold: the join completes entirely
        // in its nested-loop phase (no hash build was ever needed).
        s.defer_build = false;
        adaptive_stayed_nested_loop_ = true;
      }
      return false;
    }
    const PositionedRow& probe = s.probe_spills != nullptr
                                     ? s.spill_cache[s.probe_index]
                                     : (*s.probe_rows)[s.probe_index];
    if (s.defer_build) {
      if (++s.nl_seen > kNestedLoopProbeLimit) {
        // Adaptive switch: the outer cardinality surprise makes linear
        // probing too expensive; build the hash index and continue indexed.
        BuildShards();
        s.defer_build = false;
        adaptive_switched_ = true;
      } else {
        s.nl_row_active = true;
        s.nl_row_matched = false;
        s.nl_pos = 0;
        s.nl_probe_valid = EncodeJoinKeyInto(probe.first, *s.probe_cols,
                                             &s.nl_probe_bytes, NullSafeArg());
        s.nl_probe_null = std::ranges::any_of(*s.probe_cols, [&](slot_t col) {
          return probe.first[col].IsNull();
        });
        s.nl_probe_hash = s.nl_probe_valid ? HashBytesKey(s.nl_probe_bytes) : 0;
        continue;
      }
    }
    if (s.probe_spills == nullptr) {
      PrefetchProbeRow(*s.probe_rows, s.probe_cursor + 8);
    }
    const KeyRef k = KeyOf(probe.first, *s.probe_cols, s.key_mode,
                           s.int_mode_type, &s.scratch, NullSafeArg());
    if (!k.valid) {
      if (runtime_filter_active_) {
        probe_rows_rejected_.fetch_add(1);
        if (std::ranges::any_of(*s.probe_cols, [&](slot_t col) {
              return probe.first[col].IsNull();
            })) {
          probe_rows_null_rejected_.fetch_add(1);
        }
      }
      continue;
    }
    s.cur_shard = ShardOf(k.hash);
    s.cur_entry = s.shards[s.cur_shard].Find(k.hash, k.int_key, k.byte_key);
    if (runtime_filter_active_ && s.cur_entry == JoinHashIndex::kNil) {
      probe_rows_rejected_.fetch_add(1);
    }
  }
}

Status HashJoin::MaterializeHybrid() {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();

  const auto partition_of = [](std::string_view key) {
    return HashBytesKey(key) % kHybridPartitions;
  };

  std::vector<Row> resident_right;
  QueryMemoryCharge resident_charge;
  std::vector<SpillFile> right_spill(kHybridPartitions);
  std::vector<SpillFile> left_spill(kHybridPartitions);
  std::string key;

  Row row;
  RowPosition position;
  while (right_->Next(&row, nullptr)) {
    if (!EncodeJoinKeyInto(row, right_cols_, &key, NullSafeArg())) {
      continue;
    }
    const size_t part = partition_of(key);
    const size_t bytes = EstimateRowBytes(row);
    if (part == 0 && budget.CanReserve(bytes)) {
      resident_charge.Add(bytes);
      resident_right.push_back(std::move(row));
    } else {
      if (Status st_sp = right_spill[part].Append(row);
          st_sp != Status::kSuccess) {
        return st_sp;
      }
    }
  }
  RETURN_IF_FAIL(right_->GetStatus());

  const SideIndex resident =
      BuildSideIndex(resident_right, right_cols_, std::nullopt, NullSafeArg());
  std::string probe_scratch;

  while (left_->Next(&row, &position)) {
    if (!EncodeJoinKeyInto(row, left_cols_, &key, NullSafeArg())) {
      continue;
    }
    const size_t part = partition_of(key);
    if (part == 0 && right_spill[0].Empty()) {
      const KeyRef k = KeyOf(row, left_cols_, resident.mode, resident.int_type,
                             &probe_scratch, NullSafeArg());
      if (k.valid) {
        for (size_t e = resident.index.Find(k.hash, k.int_key, k.byte_key);
             e != JoinHashIndex::kNil; e = resident.index.ChainNext(e)) {
          output_.emplace_back(row + resident_right[resident.index.RowIndex(e)],
                               position);
        }
      }
    } else {
      if (Status st_sp = left_spill[part].Append(row, position);
          st_sp != Status::kSuccess) {
        return st_sp;
      }
    }
  }
  RETURN_IF_FAIL(left_->GetStatus());

  if (!right_spill[0].Empty()) {
    for (const Row& right_row : resident_right) {
      if (Status st_sp = right_spill[0].Append(right_row);
          st_sp != Status::kSuccess) {
        return st_sp;
      }
    }
  }
  resident_right.clear();
  resident_right.shrink_to_fit();
  resident_charge.ReleaseAll();

  for (size_t i = 0; i < kHybridPartitions; ++i) {
    if (Status st_sp = left_spill[i].FinishWriting();
        st_sp != Status::kSuccess) {
      return st_sp;
    }
    if (Status st_sp = right_spill[i].FinishWriting();
        st_sp != Status::kSuccess) {
      return st_sp;
    }
  }

  for (size_t i = 0; i < kHybridPartitions; ++i) {
    if (left_spill[i].Empty() && right_spill[i].Empty()) {
      continue;
    }
    ASSIGN_OR_RETURN(std::vector<PositionedRow>, left_part,
                     (left_spill[i].ReadAllPositioned()));
    ASSIGN_OR_RETURN(std::vector<Row>, right_part,
                     (right_spill[i].ReadAllRows()));
    JoinPartitionPair(left_part, right_part, &output_);
  }

  size_t output_bytes = 0;
  for (const auto& item : output_) {
    output_bytes += EstimateRowBytes(item.first);
  }
  output_charge_.Add(output_bytes);
  return Status::kSuccess;
}

void HashJoin::Dump(std::ostream& o, int indent) const {
  std::stringstream ss;
  ss << "left: {";
  for (size_t i = 0; i < left_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << left_cols_[i];
  }
  ss << "} right: {";
  for (size_t i = 0; i < right_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << right_cols_[i];
  }
  ss << "}";
  // Annotations that must read as part of the operator name (a multi-relation
  // EXPLAIN expects `HashJoin keys=2`, `HashJoin null_safe=true`).
  std::stringstream tags;
  if (any_null_safe_) {
    tags << " null_safe=true";
  }
  if (left_cols_.size() != 1) {
    tags << " keys=" << left_cols_.size();
  }
  std::string tail;
  if (residual_note_) {
    std::string text = residual_note_->ToString();
    if (text.size() >= 2 && text.front() == '(' && text.back() == ')') {
      text = text.substr(1, text.size() - 2);
    }
    tail = " residual: " + text;
  }
  if (kind_ == JoinKind::kSemi) {
    o << "SemiHashJoin" << tail << ": " << ss.str() << "\n"
      << Indent(static_cast<size_t>(indent) + 2);
  } else if (kind_ == JoinKind::kAnti) {
    o << "AntiHashJoin" << tail << ": " << ss.str() << "\n"
      << Indent(static_cast<size_t>(indent) + 2);
  } else if (kind_ == JoinKind::kNullAwareAnti) {
    o << "NullAwareAntiHashJoin" << tail << ": " << ss.str() << "\n"
      << Indent(static_cast<size_t>(indent) + 2);
  } else if (kind_ == JoinKind::kLeftOuter) {
    o << "LeftHashJoin" << tags.str() << tail << ": " << ss.str() << "\n"
      << Indent(static_cast<size_t>(indent) + 2);
  } else if (kind_ == JoinKind::kRightOuter) {
    o << "RightHashJoin" << tags.str() << tail << ": " << ss.str() << "\n"
      << Indent(static_cast<size_t>(indent) + 2);
  } else if (kind_ == JoinKind::kFullOuter) {
    o << "FullHashJoin" << tags.str() << tail << ": " << ss.str() << "\n"
      << Indent(static_cast<size_t>(indent) + 2);
  } else if (mode_ == HashJoinMode::kHybrid) {
    o << "HybridHashJoin" << tags.str() << tail << " (" << worker_count_
      << " workers): " << ss.str() << "\n"
      << Indent(static_cast<size_t>(indent) + 2);
  } else if (pipelined_) {
    o << "PartitionedHashJoin" << tags.str() << tail << " (pipelined, "
      << worker_count_
      << " workers, build:" << (build_left_side_ ? "left" : "right")
      << "): " << ss.str() << "\n"
      << Indent(static_cast<size_t>(indent) + 2);
  } else {
    o << "PartitionedHashJoin" << tags.str() << tail << " (" << worker_count_
      << " workers): " << ss.str() << "\n"
      << Indent(static_cast<size_t>(indent) + 2);
  }
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2);
  right_->Dump(o, indent + 2);
  if (materialized_ && semi_reduction_ &&
      (kind_ == JoinKind::kSemi || kind_ == JoinKind::kAnti ||
       kind_ == JoinKind::kNullAwareAnti)) {
    // The hash semi reduction line (Phase 2 "Hash Semi Reduction"):
    // `status` says which intake ran -- probe_key_domain, empty_probe_domain
    // or fallback=domain_cap -- and the counters say what it bought.
    o << "\n"
      << Indent(static_cast<size_t>(indent)) << "SemiReduction "
      << semi_reduction_description_
      << " probe_keys=" << semi_reduction_probe_keys_
      << " build_rows=" << semi_reduction_build_rows_
      << " build_rows_kept=" << semi_reduction_build_rows_kept_
      << " build_rows_pruned=" << SemiReductionBuildRowsPruned()
      << " duplicate_keys_pruned=" << semi_reduction_duplicate_keys_;
  }
  if (materialized_ && kind_ == JoinKind::kInner) {
    o << "\n"
      << Indent(static_cast<size_t>(indent))
      << "HashJoin actual_build_rows=" << actual_build_rows_
      << " actual_probe_rows=" << actual_probe_rows_
      << " join_matches=" << join_matches_;
    o << "\n"
      << Indent(static_cast<size_t>(indent))
      << "MemoryPeak bytes=" << memory_peak_bytes_
      << " spill_partitions=" << spill_partition_count_;
    if (runtime_filter_active_) {
      o << "\n"
        << Indent(static_cast<size_t>(indent))
        << "RuntimeFilter type=exact_set keys=" << runtime_filter_keys_ << "\n"
        << Indent(static_cast<size_t>(indent))
        << "probe_rows_rejected=" << probe_rows_rejected_.load() << "\n"
        << Indent(static_cast<size_t>(indent))
        << "probe_rows_null_rejected=" << probe_rows_null_rejected_.load();
    }
    if (dynamic_filter_published_ || dynamic_filter_rejected_ != 0) {
      // The dynamic filter is the join-published half of Phase 2
      // "Dynamic Filter Join / SIP / Bloom"; `pushdown` says whether the probe
      // scan applied it before row decode or the join applied it while
      // consuming the probe side.
      o << "\n"
        << Indent(static_cast<size_t>(indent)) << "DynamicFilter build_side="
        << (dynamic_filter_build_left_ ? "left" : "right")
        << " published=" << (dynamic_filter_published_ ? "scan" : "join") << " "
        << dynamic_filter_description_ << "\n"
        << Indent(static_cast<size_t>(indent))
        << "probe_rows_scanned=" << dynamic_filter_probe_rows_
        << " probe_rows_pruned=" << dynamic_filter_rejected_ << "\n"
        << Indent(static_cast<size_t>(indent))
        << "probe_rows_null_rejected=" << dynamic_filter_null_rejected_;
    }
    if (adaptive_switched_) {
      o << "\n"
        << Indent(static_cast<size_t>(indent))
        << "AdaptiveJoin initial=NestedLoop final=HashJoin"
        << "\n"
        << Indent(static_cast<size_t>(indent))
        << "switch_reason=outer_rows_exceeded_threshold";
    } else if (adaptive_stayed_nested_loop_) {
      o << "\n"
        << Indent(static_cast<size_t>(indent))
        << "AdaptiveJoin initial=NestedLoop final=NestedLoop"
        << "\n"
        << Indent(static_cast<size_t>(indent))
        << "switch_reason=outer_rows_below_threshold";
    }
  }
}

size_t HashJoin::MaterializedRowCount() const {
  if (!output_.empty()) {
    return output_.size();
  }
  if (state_) {
    if (state_->build_rows != nullptr) {
      return state_->build_rows->size();
    }
    return state_->right.rows.size() + state_->left.rows.size();
  }
  return 0;
}

size_t HashJoin::MaterializedBytes() const {
  if (output_charge_.Bytes() > 0) {
    return output_charge_.Bytes();
  }
  if (state_) {
    return state_->left.charge.Bytes() + state_->right.charge.Bytes();
  }
  return 0;
}

}  // namespace tinylamb
