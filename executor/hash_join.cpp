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

void AppendMemComparableValue(const Value& v, std::string* out) {
  switch (v.type) {
    case ValueType::kInt64:
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
      // Canonicalize NaN before the order-preserving transform: SQL treats
      // every NaN as equal to every other NaN (Value::operator== agrees), so
      // two NaNs that differ only in sign/payload must hash to the same join
      // key.  SortExecutor applies the identical normalization.
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
      throw std::runtime_error("Cannot encode unknown type.");
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
    if (null_safe != nullptr) {
      out->push_back('\1');
    }
    AppendMemComparableValue(value, out);
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
    if ((v.type != ValueType::kInt64 && v.type != ValueType::kDate) ||
        (found && *found != v.type)) {
      return std::nullopt;
    }
    found = v.type;
  }
  return found;
}

struct SideIndex {
  JoinHashIndex index;
  JoinHashIndex::KeyMode mode = JoinHashIndex::KeyMode::kBytes;
  ValueType int_type = ValueType::kInt64;
  bool has_null_key{false};
};

template <typename Container>
SideIndex BuildSideIndex(const Container& rows, const std::vector<slot_t>& cols,
                         const std::vector<bool>* null_safe = nullptr) {
  SideIndex side;
  // The int64 fast path cannot represent the null-presence marker, so a
  // null-safe key always uses the byte encoding.
  if (null_safe == nullptr) {
    if (auto uniform = UniformIntLikeType(rows, cols)) {
      side.mode = JoinHashIndex::KeyMode::kInt64;
      side.int_type = *uniform;
    }
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
  for (;;) {
    const size_t head = slots_[idx];
    if (head == kNil) {
      return kNil;
    }
    if (SlotKeyEquals(idx, int_key, byte_key)) {
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
    bool has_null_key{false};
    // Parallels the join's key columns (nullptr unless a null-safe key).
    const std::vector<bool>* null_safe{nullptr};

    [[nodiscard]] bool Spilled() const { return !spills.empty(); }

    void Flush(const std::vector<slot_t>& cols) {
      spills.resize(kReactiveSpillPartitions);
      std::string key;
      for (const PositionedRow& item : rows) {
        if (!EncodeJoinKeyInto(item.first, cols, &key, null_safe)) {
          spills[0].Append(item.first, item.second);
          continue;
        }
        spills[HashBytesKey(key) % kReactiveSpillPartitions].Append(
            item.first, item.second);
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
        for (auto&& item : file.ReadAllPositioned()) {
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

  static void Consume(ExecutorBase* child, const std::vector<slot_t>& cols,
                      Side* side) {
    QueryMemoryBudget& budget = QueryMemoryBudget::Global();
    std::string key;
    Row row;
    RowPosition position;
    while (child->Next(&row, &position)) {
      for (const slot_t col : cols) {
        side->has_null_key = side->has_null_key || row[col].IsNull();
      }
      const size_t bytes = EstimateRowBytes(row);
      if (!side->Spilled() && !budget.CanReserve(bytes)) {
        side->Flush(cols);
      }
      if (side->Spilled()) {
        if (!EncodeJoinKeyInto(row, cols, &key, side->null_safe)) {
          side->spills[0].Append(row, position);
          continue;
        }
        side->spills[HashBytesKey(key) % kReactiveSpillPartitions].Append(
            row, position);
      } else {
        side->charge.Add(bytes);
        side->rows.emplace_back(std::move(row), position);
      }
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

void HashJoin::MaterializeOrThrow() {
  if (materialize_failed_) {
    throw std::runtime_error("hash join materialization previously failed");
  }
  try {
    Materialize();
  } catch (...) {
    materialize_failed_ = true;
    throw;
  }
}

bool HashJoin::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) {
    MaterializeOrThrow();
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
    MaterializeOrThrow();
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

void HashJoin::Materialize() {
  output_.clear();
  output_offset_ = 0;
  if (kind_ == JoinKind::kLeftOuter || kind_ == JoinKind::kRightOuter ||
      kind_ == JoinKind::kFullOuter) {
    MaterializeOuter();
    pipelined_ = false;
    materialized_ = true;
    return;
  }
  if (kind_ == JoinKind::kSingle) {
    MaterializeSingle();
    pipelined_ = false;
    materialized_ = true;
    return;
  }
  if (kind_ == JoinKind::kMark) {
    MaterializeMarkJoin();
    pipelined_ = false;
    materialized_ = true;
    return;
  }
  if (kind_ != JoinKind::kInner) {
    MaterializeSemiAnti();
    pipelined_ = false;
    materialized_ = true;
    return;
  }
  if (mode_ == HashJoinMode::kHybrid) {
    MaterializeHybrid();
    pipelined_ = false;
  } else {
    MaterializeInMemory();
    pipelined_ = true;
  }
  materialized_ = true;
}

void HashJoin::MaterializeSingle() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  IntakeBothSides();
  // D8 (docs/design.md): the single-row (scalar subquery) join cannot stream
  // partition-by-partition, so reload any spilled input rather than dropping
  // the spilled rows and returning a wrong (missing) result.
  s.left.LoadAll();
  s.right.LoadAll();
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();

  const std::vector<PositionedRow>& left_rows = s.left.rows;
  const std::vector<PositionedRow>& right_rows = s.right.rows;
  std::string scratch;

  const SideIndex build =
      BuildSideIndex(right_rows, right_cols_, NullSafeArg());
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
        throw std::runtime_error(
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
}

void HashJoin::MaterializeMarkJoin() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  IntakeBothSides();
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();

  std::string scratch;
  size_t output_bytes = 0;
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
        BuildSideIndex(s.right.rows, right_cols_, NullSafeArg());
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
        process_probe_rows(part.ReadAllPositioned());
      }
    }
    output_charge_.Add(output_bytes);
    return;
  }

  const auto process_probe_spilled =
      [&](const std::vector<PositionedRow>& rows) {
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
          std::vector<PositionedRow> right_part =
              s.right.spills[p].ReadAllPositioned();
          if (right_part.empty()) {
            continue;
          }
          const SideIndex build =
              BuildSideIndex(right_part, right_cols_, NullSafeArg());
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
      };

  if (!s.left.Spilled()) {
    process_probe_spilled(s.left.rows);
  } else {
    for (SpillFile& part : s.left.spills) {
      process_probe_spilled(part.ReadAllPositioned());
    }
  }
  output_charge_.Add(output_bytes);
}

void HashJoin::MaterializeOuter() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  IntakeBothSides();
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();

  std::vector<PositionedRow> left_rows;
  left_rows.reserve(s.left.rows.size());
  for (const PositionedRow& row : s.left.rows) {
    left_rows.push_back(row);
  }
  for (SpillFile& part : s.left.spills) {
    std::vector<PositionedRow> rows = part.ReadAllPositioned();
    left_rows.insert(left_rows.end(), rows.begin(), rows.end());
  }
  std::vector<PositionedRow> right_rows;
  right_rows.reserve(s.right.rows.size());
  for (const PositionedRow& row : s.right.rows) {
    right_rows.push_back(row);
  }
  for (SpillFile& part : s.right.spills) {
    std::vector<PositionedRow> rows = part.ReadAllPositioned();
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

  if (preserve_left) {
    const SideIndex build =
        BuildSideIndex(right_rows, right_cols_, NullSafeArg());
    for (const auto& left : left_rows) {
      const KeyRef key = KeyOf(left.first, left_cols_, build.mode,
                               build.int_type, &scratch, NullSafeArg());
      const size_t first =
          key.valid ? build.index.Find(key.hash, key.int_key, key.byte_key)
                    : JoinHashIndex::kNil;
      if (first == JoinHashIndex::kNil) {
        output_.emplace_back(left.first + Row(std::vector<Value>(right_width_)),
                             left.second);
        continue;
      }
      for (size_t entry = first; entry != JoinHashIndex::kNil;
           entry = build.index.ChainNext(entry)) {
        const size_t right_index = build.index.RowIndex(entry);
        matched_right[right_index] = true;
        output_.emplace_back(left.first + right_rows[right_index].first,
                             left.second);
      }
    }
  } else {
    const SideIndex build =
        BuildSideIndex(left_rows, left_cols_, NullSafeArg());
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
}

void HashJoin::MaterializeSemiAnti() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  IntakeBothSides();
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();
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

  if (kind_ == JoinKind::kNullAwareAnti && s.right.has_null_key) {
    // NOT IN is UNKNOWN for every non-matching probe when the build set has a
    // NULL key; UNKNOWN is filtered out by a surrounding WHERE.
    return;
  }

  // Raw build-side cardinality decides how NULL probe keys behave: an empty
  // set makes `x NOT IN S` true for every x (anti emits everything), while a
  // non-empty set turns it into UNKNOWN unless x is found (NULL probes are
  // then dropped). Callers decorrelating NOT IN must guarantee a NOT NULL
  // build key so this case cannot arise (see optimizer.cpp).
  size_t build_total = s.right.rows.size();
  for (const SpillFile& part : s.right.spills) {
    build_total += part.Count();
  }
  if (!semi && build_total == 0) {
    if (!s.left.Spilled()) {
      for (const PositionedRow& probe : s.left.rows) {
        emit_probe(probe);
      }
    } else {
      for (SpillFile& part : s.left.spills) {
        for (const PositionedRow& probe : part.ReadAllPositioned()) {
          emit_probe(probe);
        }
      }
    }
    output_charge_.Add(output_bytes);
    return;
  }

  if (!s.right.Spilled()) {
    // Resident build side: index it once and stream every left partition
    // through an existence check.
    const SideIndex build =
        BuildSideIndex(s.right.rows, right_cols_, NullSafeArg());
    const auto stream_left = [&](const std::vector<PositionedRow>& rows) {
      for (const PositionedRow& probe : rows) {
        if (probe_survives(probe_lookup(probe, build))) {
          emit_probe(probe);
        }
      }
    };
    if (!s.left.Spilled()) {
      stream_left(s.left.rows);
    } else {
      for (SpillFile& part : s.left.spills) {
        stream_left(part.ReadAllPositioned());
      }
    }
    output_charge_.Add(output_bytes);
    return;
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
      std::vector<PositionedRow> right_part =
          s.right.spills[p].ReadAllPositioned();
      if (right_part.empty()) {
        continue;
      }
      const SideIndex build =
          BuildSideIndex(right_part, right_cols_, NullSafeArg());
      for (size_t i = 0; i < s.left.rows.size(); ++i) {
        if (matched[i] == ProbeState::kMatch) {
          continue;
        }
        const int state = probe_lookup(s.left.rows[i], build);
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
      std::vector<PositionedRow> right_part =
          s.right.spills[p].ReadAllPositioned();
      std::vector<PositionedRow> left_part =
          s.left.spills[p].ReadAllPositioned();
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
      const SideIndex build =
          BuildSideIndex(right_part, right_cols_, NullSafeArg());
      for (const PositionedRow& probe : left_part) {
        if (probe_survives(probe_lookup(probe, build))) {
          emit_probe(probe);
        }
      }
    }
  }
  output_charge_.Add(output_bytes);
}

void HashJoin::IntakeBothSides() {
  JoinState& s = *state_;
  s.left.null_safe = NullSafeArg();
  s.right.null_safe = NullSafeArg();
  std::exception_ptr left_error;
  std::exception_ptr right_error;
  std::jthread left_thread([&] {
    try {
      JoinState::Consume(left_.get(), left_cols_, &s.left);
    } catch (...) {
      left_error = std::current_exception();
    }
  });
  try {
    JoinState::Consume(right_.get(), right_cols_, &s.right);
  } catch (...) {
    right_error = std::current_exception();
  }
  left_thread.join();
  if (left_error) {
    std::rethrow_exception(left_error);
  }
  if (right_error) {
    std::rethrow_exception(right_error);
  }
}

void HashJoin::BuildShards() {
  JoinState& s = *state_;
  const std::vector<PositionedRow>& rows = *s.build_rows;
  if (s.build_cols == nullptr) {
    throw std::runtime_error("hash join build side is not configured");
  }
  const std::vector<slot_t>& cols = *s.build_cols;
  if (auto uniform = NullSafeArg() == nullptr ? UniformIntLikeType(rows, cols)
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
      s.spill_cache =
          (*s.probe_spills)[s.spill_partition++].ReadAllPositioned();
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
  s.left_builds = SumRowBytes(s.left.rows) < SumRowBytes(s.right.rows);
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
  const SideIndex build =
      left_builds ? BuildSideIndex(left_part, left_cols_, NullSafeArg())
                  : BuildSideIndex(right_part, right_cols_, NullSafeArg());
  const std::vector<slot_t>& probe_cols =
      left_builds ? right_cols_ : left_cols_;
  std::string scratch;
  if (left_builds) {
    for (const auto& probe_row : right_part) {
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
    for (const PositionedRow& probe : left_part) {
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

void HashJoin::SetupBothSpilled() {
  JoinState& s = *state_;
  s.part_outputs.assign(kReactiveSpillPartitions, {});
  const size_t workers = std::min(worker_count_, kReactiveSpillPartitions);
  std::atomic<size_t> next_partition{0};
  std::exception_ptr error;
  std::mutex error_mutex;
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  for (size_t worker = 0; worker < workers; ++worker) {
    threads.emplace_back([&] {
      try {
        for (;;) {
          const size_t p = next_partition.fetch_add(1);
          if (p >= kReactiveSpillPartitions) {
            break;
          }
          auto left_part = s.left.spills[p].ReadAllPositioned();
          auto right_part = s.right.spills[p].ReadAllPositioned();
          JoinPartitionPair(left_part, right_part, &s.part_outputs[p]);
        }
      } catch (...) {
        std::scoped_lock lock(error_mutex);
        if (!error) {
          error = std::current_exception();
        }
      }
    });
  }
  threads.clear();
  if (error) {
    std::rethrow_exception(error);
  }
  size_t output_bytes = 0;
  for (const auto& part : s.part_outputs) {
    for (const auto& item : part) {
      output_bytes += EstimateRowBytes(item.first);
    }
  }
  output_charge_.Add(output_bytes);
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
    threads.emplace_back([&, worker, begin, end] {
      try {
        std::string scratch;
        for (size_t i = begin; i < end; ++i) {
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

void HashJoin::MaterializeInMemory() {
  state_ = std::make_unique<JoinState>();
  IntakeBothSides();
  JoinState& s = *state_;
  const bool left_spilled = s.left.Spilled();
  const bool right_spilled = s.right.Spilled();
  if (left_spilled && right_spilled) {
    SetupBothSpilled();
  } else if (left_spilled || right_spilled) {
    SetupOneSideSpilled();
  } else {
    SetupInMemoryJoin();
  }
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

void HashJoin::MaterializeHybrid() {
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
      right_spill[part].Append(row);
    }
  }

  const SideIndex resident =
      BuildSideIndex(resident_right, right_cols_, NullSafeArg());
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
      left_spill[part].Append(row, position);
    }
  }

  if (!right_spill[0].Empty()) {
    for (const Row& right_row : resident_right) {
      right_spill[0].Append(right_row);
    }
  }
  resident_right.clear();
  resident_right.shrink_to_fit();
  resident_charge.ReleaseAll();

  for (size_t i = 0; i < kHybridPartitions; ++i) {
    left_spill[i].FinishWriting();
    right_spill[i].FinishWriting();
  }

  for (size_t i = 0; i < kHybridPartitions; ++i) {
    if (left_spill[i].Empty() && right_spill[i].Empty()) {
      continue;
    }
    auto left_part = left_spill[i].ReadAllPositioned();
    auto right_part = right_spill[i].ReadAllRows();
    JoinPartitionPair(left_part, right_part, &output_);
  }

  size_t output_bytes = 0;
  for (const auto& item : output_) {
    output_bytes += EstimateRowBytes(item.first);
  }
  output_charge_.Add(output_bytes);
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
    // Adaptive re-optimization decision record: the plan completed with the
    // remaining work below the re-optimization threshold, so the original
    // physical plan was retained.
    o << "\n"
      << Indent(static_cast<size_t>(indent)) << "Reoptimization considered=true"
      << " retained_original=true"
      << "\n"
      << Indent(static_cast<size_t>(indent))
      << "reason=remaining_work_below_threshold";
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
