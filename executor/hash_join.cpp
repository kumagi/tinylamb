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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
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
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

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
      char buf[8];
      std::memcpy(buf, &be, sizeof(buf));
      buf[0] ^= static_cast<char>(0x80);
      out->append(buf, sizeof(buf));
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
      uint64_t bits = 0;
      std::memcpy(&bits, &v.value.double_value, sizeof(bits));
      uint64_t be = BSwap64(bits);
      if (0 <= v.value.double_value) {
        be |= 0x80;
      } else {
        be = ~be;
      }
      char buf[8];
      std::memcpy(buf, &be, sizeof(buf));
      out->append(buf, sizeof(buf));
      break;
    }
    case ValueType::kArray:
      out->append(v.EncodeMemcomparableFormat());
      break;
    case ValueType::kNull:
      throw std::runtime_error("Cannot encode unknown type.");
  }
}

bool EncodeJoinKeyInto(const Row& row, const std::vector<slot_t>& cols,
                       std::string* out) {
  out->clear();
  for (const slot_t col : cols) {
    const Value& value = row[col];
    if (value.IsNull()) {
      return false;
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
             std::string* scratch) {
  if (mode == JoinHashIndex::KeyMode::kInt64) {
    const Value& v = row[cols[0]];
    if (v.IsNull() || v.type != int_mode_type) { return {};
}
    return {true, v.value.int_value, {}, HashInt64Key(v.value.int_value)};
  }
  if (!EncodeJoinKeyInto(row, cols, scratch)) { return {};
}
  return {true, 0, std::string_view(*scratch), HashBytesKey(*scratch)};
}

const Row& RowOf(const Row& row) { return row; }
const Row& RowOf(const PositionedRow& positioned) { return positioned.first; }

template <typename Container>
std::optional<ValueType> UniformIntLikeType(const Container& rows,
                                            const std::vector<slot_t>& cols) {
  if (cols.size() != 1) { return std::nullopt;
}
  std::optional<ValueType> found;
  for (const auto& item : rows) {
    const Value& v = RowOf(item)[cols[0]];
    if (v.IsNull()) { continue;
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
};

template <typename Container>
SideIndex BuildSideIndex(const Container& rows,
                         const std::vector<slot_t>& cols) {
  SideIndex side;
  if (auto uniform = UniformIntLikeType(rows, cols)) {
    side.mode = JoinHashIndex::KeyMode::kInt64;
    side.int_type = *uniform;
  }
  side.index.Init(side.mode, rows.size());
  std::string scratch;
  for (size_t i = 0; i < rows.size(); ++i) {
    const KeyRef k =
        KeyOf(RowOf(rows[i]), cols, side.mode, side.int_type, &scratch);
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
  const uint32_t off = static_cast<uint32_t>(arena_.size());
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
      entries_.push_back(Entry{row_index, kNil});
      return;
    }
    if (SlotKeyEquals(idx, int_key, byte_key)) {
      entries_.push_back(Entry{row_index, slots_[idx]});
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
    if (head == kNil) { return kNil;
}
    if (SlotKeyEquals(idx, int_key, byte_key)) { return head;
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
    if (slots_[old] == kNil) { continue;
}
    const uint64_t hash =
        mode_ == KeyMode::kInt64
            ? HashInt64Key(slot_int_keys_[old])
            : HashBytesKey(std::string_view(
                  arena_.data() + slot_byte_keys_[old].first,
                  slot_byte_keys_[old].second));
    size_t idx = hash & new_mask;
    while (new_slots[idx] != kNil) { idx = (idx + 1) & new_mask;
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

    [[nodiscard]] bool Spilled() const { return !spills.empty(); }

    void Flush(const std::vector<slot_t>& cols) {
      spills.resize(kReactiveSpillPartitions);
      std::string key;
      for (const PositionedRow& item : rows) {
        if (!EncodeJoinKeyInto(item.first, cols, &key)) { continue;
}
        spills[HashBytesKey(key) % kReactiveSpillPartitions].Append(
            item.first, item.second);
      }
      charge.ReleaseAll();
      rows.clear();
      rows.shrink_to_fit();
    }
  };

  Side left;
  Side right;
  JoinHashIndex::KeyMode key_mode = JoinHashIndex::KeyMode::kBytes;
  ValueType int_mode_type = ValueType::kInt64;

  const std::vector<PositionedRow>* build_rows = nullptr;
  const std::vector<slot_t>* build_cols = nullptr;
  const std::vector<slot_t>* probe_cols = nullptr;
  bool left_builds = false;

  std::vector<JoinHashIndex> shards;
  uint32_t shard_bits = 0;

  const std::vector<PositionedRow>* probe_rows = nullptr;
  std::vector<SpillFile>* probe_spills = nullptr;
  size_t probe_cursor = 0;
  size_t probe_index = 0;
  size_t spill_partition = 0;
  size_t spill_cursor = 0;
  std::vector<PositionedRow> spill_cache;
  bool have_probe_row = false;
  size_t cur_shard = 0;
  size_t cur_entry = JoinHashIndex::kNil;

  std::vector<std::vector<PositionedRow>> stripe_outputs;
  std::vector<std::vector<PositionedRow>> part_outputs;
  size_t queue_index = 0;
  size_t queue_offset = 0;

  std::string scratch;

  void Consume(ExecutorBase* child, const std::vector<slot_t>& cols,
               Side* side) {
    QueryMemoryBudget& budget = QueryMemoryBudget::Global();
    std::string key;
    Row row;
    RowPosition position;
    while (child->Next(&row, &position)) {
      const size_t bytes = EstimateRowBytes(row);
      if (!side->Spilled() && !budget.CanReserve(bytes)) {
        side->Flush(cols);
      }
      if (side->Spilled()) {
        if (!EncodeJoinKeyInto(row, cols, &key)) { continue;
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

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols,
                   Executor right, std::vector<slot_t> right_cols,
                   size_t worker_count)
    : HashJoin(std::move(left), std::move(left_cols), std::move(right),
               std::move(right_cols), HashJoinMode::kInMemory, worker_count) {}

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols,
                   Executor right, std::vector<slot_t> right_cols,
                   HashJoinMode mode, size_t worker_count)
    : HashJoin(std::move(left), std::move(left_cols), std::move(right),
               std::move(right_cols), mode, JoinKind::kInner, worker_count) {}

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols,
                   Executor right, std::vector<slot_t> right_cols,
                   HashJoinMode mode, JoinKind kind, size_t worker_count)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)),
      mode_(mode),
      kind_(kind),
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
  while (output_offset_ < output_.size() &&
         destination->Size() < max_rows) {
    destination->Append(output_[output_offset_].first,
                        output_[output_offset_].second);
    ++output_offset_;
  }
  return destination->Size();
}

void HashJoin::Materialize() {
  output_.clear();
  output_offset_ = 0;
  if (kind_ == JoinKind::kSemi || kind_ == JoinKind::kAnti) {
    MaterializeSemiAnti();
    pipelined_ = false;
    materialized_ = true;
    return;
  }
  if (kind_ == JoinKind::kLeftOuter) {
    MaterializeLeftOuter();
    pipelined_ = false;
    materialized_ = true;
    return;
  }
  if (kind_ == JoinKind::kRightOuter) {
    MaterializeRightOuter();
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
    const KeyRef k =
        KeyOf(probe.first, left_cols_, build.mode, build.int_type, &scratch);
    if (!k.valid) { return -1;
}
    return build.index.Find(k.hash, k.int_key, k.byte_key) !=
                   JoinHashIndex::kNil
               ? 1
               : 0;
  };

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
      for (const PositionedRow& probe : s.left.rows) { emit_probe(probe);
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
    const SideIndex build = BuildSideIndex(s.right.rows, right_cols_);
    const auto stream_left = [&](const std::vector<PositionedRow>& rows) {
      for (const PositionedRow& probe : rows) {
        if (probe_lookup(probe, build) == static_cast<int>(semi)) {
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
    // flags accumulate across partitions before emission.
    std::vector<bool> matched(s.left.rows.size(), false);
    for (size_t p = 0; p < kReactiveSpillPartitions; ++p) {
      std::vector<PositionedRow> right_part =
          s.right.spills[p].ReadAllPositioned();
      if (right_part.empty()) { continue;
}
      const SideIndex build = BuildSideIndex(right_part, right_cols_);
      for (size_t i = 0; i < s.left.rows.size(); ++i) {
        if (matched[i]) { continue;
}
        matched[i] = probe_lookup(s.left.rows[i], build) == 1;
      }
    }
    for (size_t i = 0; i < s.left.rows.size(); ++i) {
      if (matched[i] == semi) { emit_probe(s.left.rows[i]); }
    }
  } else {
    for (size_t p = 0; p < kReactiveSpillPartitions; ++p) {
      std::vector<PositionedRow> right_part =
          s.right.spills[p].ReadAllPositioned();
      std::vector<PositionedRow> left_part =
          s.left.spills[p].ReadAllPositioned();
      if (left_part.empty()) { continue;
}
      if (right_part.empty()) {
        if (!semi) {
          for (const PositionedRow& probe : left_part) { emit_probe(probe);
}
        }
        continue;
      }
      const SideIndex build = BuildSideIndex(right_part, right_cols_);
      for (const PositionedRow& probe : left_part) {
        if (probe_lookup(probe, build) == static_cast<int>(semi)) {
          emit_probe(probe);
        }
      }
    }
  }
  output_charge_.Add(output_bytes);
}

// Left outer hash join: build an index on the right side, probe with the
// left side.  Every left row that matches at least one right row emits
// joined pairs (like inner).  Every left row with no match (or a NULL
// key) emits left + NULL-padded right columns so the left row survives.
void HashJoin::MaterializeLeftOuter() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  IntakeBothSides();
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();

  // NULL pad row: one Value() (kNull) per right-side column.
  const size_t right_col_count = right_cols_.empty()
                                     ? (s.right.rows.empty()
                                            ? 0
                                            : s.right.rows[0].first.Size())
                                     : right_cols_.size();
  Row null_pad;
  null_pad.values_.assign(right_col_count, Value());

  size_t output_bytes = 0;
  const auto emit_match = [&](const PositionedRow& left,
                              const PositionedRow& right_row) {
    const Row joined = left.first + right_row.first;
    output_bytes += EstimateRowBytes(joined);
    output_.emplace_back(std::move(joined), left.second);
  };
  const auto emit_unmatched = [&](const PositionedRow& left) {
    const Row joined = left.first + null_pad;
    output_bytes += EstimateRowBytes(joined);
    output_.emplace_back(std::move(joined), left.second);
  };
  std::string scratch;

  // Build index on right side (always the build side for outer joins).
  if (!s.right.Spilled()) {
    const SideIndex build = BuildSideIndex(s.right.rows, right_cols_);
    if (!s.left.Spilled()) {
      for (const PositionedRow& probe : s.left.rows) {
        const KeyRef k = KeyOf(probe.first, left_cols_, build.mode,
                               build.int_type, &scratch);
        bool matched = false;
        if (k.valid) {
          for (size_t e = build.index.Find(k.hash, k.int_key, k.byte_key);
               e != JoinHashIndex::kNil; e = build.index.ChainNext(e)) {
            emit_match(probe, s.right.rows[build.index.RowIndex(e)]);
            matched = true;
          }
        }
        if (!matched) { emit_unmatched(probe); }
      }
    } else {
      for (SpillFile& part : s.left.spills) {
        for (const PositionedRow& probe : part.ReadAllPositioned()) {
          const KeyRef k = KeyOf(probe.first, left_cols_, build.mode,
                                 build.int_type, &scratch);
          bool matched = false;
          if (k.valid) {
            for (size_t e =
                     build.index.Find(k.hash, k.int_key, k.byte_key);
                 e != JoinHashIndex::kNil;
                 e = build.index.ChainNext(e)) {
              emit_match(probe, s.right.rows[build.index.RowIndex(e)]);
              matched = true;
            }
          }
          if (!matched) { emit_unmatched(probe); }
        }
      }
    }
  } else {
    // Right side spilled: partition both sides and join per-partition.
    std::vector<bool> left_matched;
    if (!s.left.Spilled()) {
      left_matched.assign(s.left.rows.size(), false);
    }
    for (size_t p = 0; p < kReactiveSpillPartitions; ++p) {
      std::vector<PositionedRow> right_part =
          s.right.spills[p].ReadAllPositioned();
      if (right_part.empty()) { continue; }
      const SideIndex build = BuildSideIndex(right_part, right_cols_);
      if (!s.left.Spilled()) {
        for (size_t i = 0; i < s.left.rows.size(); ++i) {
          const PositionedRow& probe = s.left.rows[i];
          const KeyRef k = KeyOf(probe.first, left_cols_, build.mode,
                                 build.int_type, &scratch);
          if (k.valid) {
            for (size_t e =
                     build.index.Find(k.hash, k.int_key, k.byte_key);
                 e != JoinHashIndex::kNil;
                 e = build.index.ChainNext(e)) {
              emit_match(probe, right_part[build.index.RowIndex(e)]);
              left_matched[i] = true;
            }
          }
        }
      } else {
        for (SpillFile& part : s.left.spills) {
          for (const PositionedRow& probe : part.ReadAllPositioned()) {
            const KeyRef k = KeyOf(probe.first, left_cols_, build.mode,
                                   build.int_type, &scratch);
            if (k.valid) {
              for (size_t e = build.index.Find(k.hash, k.int_key,
                                               k.byte_key);
                   e != JoinHashIndex::kNil;
                   e = build.index.ChainNext(e)) {
                emit_match(probe, right_part[build.index.RowIndex(e)]);
              }
            }
          }
        }
      }
    }
    if (!s.left.Spilled()) {
      for (size_t i = 0; i < s.left.rows.size(); ++i) {
        if (!left_matched[i]) { emit_unmatched(s.left.rows[i]); }
      }
    }
  }
  output_charge_.Add(output_bytes);
}

void HashJoin::MaterializeRightOuter() {
  state_ = std::make_unique<JoinState>();
  JoinState& s = *state_;
  IntakeBothSides();
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();

  // NULL pad row: one Value() (kNull) per left-side column.
  const size_t left_col_count = left_cols_.empty()
                                    ? (s.left.rows.empty()
                                           ? 0
                                           : s.left.rows[0].first.Size())
                                    : left_cols_.size();
  Row null_pad;
  null_pad.values_.assign(left_col_count, Value());

  size_t output_bytes = 0;
  // Right outer: build index on LEFT side, probe with RIGHT side.
  // Emit right + left on match, right + NULL-pad-left on no match.
  const auto emit_match = [&](const PositionedRow& right_row,
                              const PositionedRow& left_row) {
    const Row joined = right_row.first + left_row.first;
    output_bytes += EstimateRowBytes(joined);
    output_.emplace_back(std::move(joined), right_row.second);
  };
  const auto emit_unmatched = [&](const PositionedRow& right_row) {
    const Row joined = null_pad + right_row.first;
    output_bytes += EstimateRowBytes(joined);
    output_.emplace_back(std::move(joined), right_row.second);
  };
  std::string scratch;

  if (!s.left.Spilled()) {
    const SideIndex build = BuildSideIndex(s.left.rows, left_cols_);
    if (!s.right.Spilled()) {
      for (const PositionedRow& probe : s.right.rows) {
        const KeyRef k = KeyOf(probe.first, right_cols_, build.mode,
                               build.int_type, &scratch);
        bool matched = false;
        if (k.valid) {
          for (size_t e = build.index.Find(k.hash, k.int_key, k.byte_key);
               e != JoinHashIndex::kNil; e = build.index.ChainNext(e)) {
            emit_match(probe, s.left.rows[build.index.RowIndex(e)]);
            matched = true;
          }
        }
        if (!matched) { emit_unmatched(probe); }
      }
    } else {
      for (SpillFile& part : s.right.spills) {
        for (const PositionedRow& probe : part.ReadAllPositioned()) {
          const KeyRef k = KeyOf(probe.first, right_cols_, build.mode,
                                 build.int_type, &scratch);
          bool matched = false;
          if (k.valid) {
            for (size_t e = build.index.Find(k.hash, k.int_key, k.byte_key);
                 e != JoinHashIndex::kNil;
                 e = build.index.ChainNext(e)) {
              emit_match(probe, s.left.rows[build.index.RowIndex(e)]);
              matched = true;
            }
          }
          if (!matched) { emit_unmatched(probe); }
        }
      }
    }
  } else {
    std::vector<bool> right_matched;
    if (!s.right.Spilled()) {
      right_matched.assign(s.right.rows.size(), false);
    }
    for (size_t p = 0; p < kReactiveSpillPartitions; ++p) {
      std::vector<PositionedRow> left_part =
          s.left.spills[p].ReadAllPositioned();
      if (left_part.empty()) { continue; }
      const SideIndex build = BuildSideIndex(left_part, left_cols_);
      if (!s.right.Spilled()) {
        for (size_t i = 0; i < s.right.rows.size(); ++i) {
          const PositionedRow& probe = s.right.rows[i];
          const KeyRef k = KeyOf(probe.first, right_cols_, build.mode,
                                 build.int_type, &scratch);
          if (k.valid) {
            for (size_t e = build.index.Find(k.hash, k.int_key, k.byte_key);
                 e != JoinHashIndex::kNil;
                 e = build.index.ChainNext(e)) {
              emit_match(probe, left_part[build.index.RowIndex(e)]);
              right_matched[i] = true;
            }
          }
        }
      } else {
        for (SpillFile& part : s.right.spills) {
          for (const PositionedRow& probe : part.ReadAllPositioned()) {
            const KeyRef k = KeyOf(probe.first, right_cols_, build.mode,
                                   build.int_type, &scratch);
            if (k.valid) {
              for (size_t e = build.index.Find(k.hash, k.int_key, k.byte_key);
                   e != JoinHashIndex::kNil;
                   e = build.index.ChainNext(e)) {
                emit_match(probe, left_part[build.index.RowIndex(e)]);
              }
            }
          }
        }
      }
    }
    if (!s.right.Spilled()) {
      for (size_t i = 0; i < s.right.rows.size(); ++i) {
        if (!right_matched[i]) { emit_unmatched(s.right.rows[i]); }
      }
    }
  }
  output_charge_.Add(output_bytes);
}

void HashJoin::IntakeBothSides() {
  JoinState& s = *state_;
  std::exception_ptr left_error;
  std::exception_ptr right_error;
  std::jthread left_thread([&] {
    try {
      s.Consume(left_.get(), left_cols_, &s.left);
    } catch (...) {
      left_error = std::current_exception();
    }
  });
  try {
    s.Consume(right_.get(), right_cols_, &s.right);
  } catch (...) {
    right_error = std::current_exception();
  }
  left_thread.join();
  if (left_error) { std::rethrow_exception(left_error);
}
  if (right_error) { std::rethrow_exception(right_error);
}
}

void HashJoin::BuildShards() {
  JoinState& s = *state_;
  const std::vector<PositionedRow>& rows = *s.build_rows;
  if (s.build_cols == nullptr) {
    throw std::runtime_error("hash join build side is not configured");
  }
  const std::vector<slot_t>& cols = *s.build_cols;
  if (auto uniform = UniformIntLikeType(rows, cols)) {
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
    while ((size_t{1} << bits) < want) { ++bits;
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
    const KeyRef k =
        KeyOf(rows[i].first, cols, s.key_mode, s.int_mode_type, &scratch);
    hashes[i] = k.hash;
    ++counts[shard_of(hashes[i]) + 1];
  }
  for (size_t i = 0; i < shard_count; ++i) { counts[i + 1] += counts[i];
}
  std::vector<size_t> sizes(shard_count, 0);
  for (size_t i = 0; i < rows.size(); ++i) { ++sizes[shard_of(hashes[i])];
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
                             &local_scratch);
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
    const size_t end =
        k + 1 < shard_count ? counts[k + 1] : ordered.size();
    threads.emplace_back([&, begin, end] {
      try {
        fill_range(begin, end);
      } catch (...) {
        std::scoped_lock lock(error_mutex);
        if (!error) { error = std::current_exception();
}
      }
    });
  }
  threads.clear();
  if (error) { std::rethrow_exception(error);
}
}

uint32_t HashJoin::ShardOf(uint64_t hash) const {
  if (state_->shard_bits == 0) { return 0;
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
      s.spill_cache = (*s.probe_spills)[s.spill_partition++].ReadAllPositioned();
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
  s.left_builds =
      SumRowBytes(s.left.rows) < SumRowBytes(s.right.rows);
  build_left_side_ = s.left_builds;
  if (s.left_builds) {
    s.build_rows = &s.left.rows;
    s.build_cols = &left_cols_;
    BuildShards();
    s.probe_rows = &s.right.rows;
    s.probe_cols = &right_cols_;
  } else {
    s.build_rows = &s.right.rows;
    s.build_cols = &right_cols_;
    BuildShards();
    s.probe_rows = &s.left.rows;
    s.probe_cols = &left_cols_;
  }
  s.left.charge.ReleaseAll();
  s.right.charge.ReleaseAll();
  if (worker_count_ > 1 && s.probe_rows->size() >= kParallelProbeMinRows) {
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
  if (left_part.empty() || right_part.empty()) { return;
}
  const bool left_builds =
      SumRowBytes(left_part) < SumRowBytes(right_part);
  const SideIndex build =
      left_builds ? BuildSideIndex(left_part, left_cols_)
                  : BuildSideIndex(right_part, right_cols_);
  const std::vector<slot_t>& probe_cols = left_builds ? right_cols_ : left_cols_;
  std::string scratch;
  if (left_builds) {
    for (const auto& probe_row : right_part) {
      const KeyRef k = KeyOf(RowOf(probe_row), probe_cols, build.mode,
                             build.int_type, &scratch);
      if (!k.valid) { continue;
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
                             build.int_type, &scratch);
      if (!k.valid) { continue;
}
      for (size_t e = build.index.Find(k.hash, k.int_key, k.byte_key);
           e != JoinHashIndex::kNil; e = build.index.ChainNext(e)) {
        out->emplace_back(probe.first +
                              RowOf(right_part[build.index.RowIndex(e)]),
                          probe.second);
      }
    }
  }
}

void HashJoin::SetupBothSpilled() {
  JoinState& s = *state_;
  s.part_outputs.assign(kReactiveSpillPartitions, {});
  const size_t workers =
      std::min(worker_count_, kReactiveSpillPartitions);
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
          if (p >= kReactiveSpillPartitions) { break;
}
          auto left_part = s.left.spills[p].ReadAllPositioned();
          auto right_part = s.right.spills[p].ReadAllPositioned();
          JoinPartitionPair(left_part, right_part, &s.part_outputs[p]);
        }
      } catch (...) {
        std::scoped_lock lock(error_mutex);
        if (!error) { error = std::current_exception();
}
      }
    });
  }
  threads.clear();
  if (error) { std::rethrow_exception(error); }
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
    if (begin >= end) { break;
}
    threads.emplace_back([&, worker, begin, end] {
      try {
        std::string scratch;
        for (size_t i = begin; i < end; ++i) {
          const PositionedRow& probe = (*s.probe_rows)[i];
          const KeyRef k = KeyOf(probe.first, *s.probe_cols, s.key_mode,
                                 s.int_mode_type, &scratch);
          if (!k.valid) { continue;
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
        if (!error) { error = std::current_exception();
}
      }
    });
  }
  threads.clear();
  if (error) { std::rethrow_exception(error); }
  size_t output_bytes = 0;
  for (const auto& stripe : outs) {
    for (const auto& item : stripe) {
      output_bytes += EstimateRowBytes(item.first);
    }
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
    auto& queues =
        s.stripe_outputs.empty() ? s.part_outputs : s.stripe_outputs;
    while (s.queue_index < queues.size()) {
      auto& queue = queues[s.queue_index];
      if (s.queue_offset < queue.size()) {
        PositionedRow& item = queue[s.queue_offset++];
        *dst = std::move(item.first);
        if (rp != nullptr) { *rp = item.second;
}
        return true;
      }
      ++s.queue_index;
      s.queue_offset = 0;
    }
    return false;
  }

  while (true) {
    if (s.have_probe_row && s.cur_entry != JoinHashIndex::kNil) {
      const PositionedRow& probe = s.probe_spills != nullptr
                                       ? s.spill_cache[s.probe_index]
                                       : (*s.probe_rows)[s.probe_index];
      const PositionedRow& b =
          (*s.build_rows)[s.shards[s.cur_shard].RowIndex(s.cur_entry)];
      if (s.left_builds) {
        *dst = b.first + probe.first;
        if (rp != nullptr) { *rp = b.second;
}
      } else {
        *dst = probe.first + b.first;
        if (rp != nullptr) { *rp = probe.second;
}
      }
      s.cur_entry = s.shards[s.cur_shard].ChainNext(s.cur_entry);
      return true;
    }
    if (!FetchNextProbe()) { return false;
}
    const PositionedRow& probe = s.probe_spills != nullptr
                                     ? s.spill_cache[s.probe_index]
                                     : (*s.probe_rows)[s.probe_index];
    const KeyRef k = KeyOf(probe.first, *s.probe_cols, s.key_mode,
                           s.int_mode_type, &s.scratch);
    if (!k.valid) { continue;
}
    s.cur_shard = ShardOf(k.hash);
    s.cur_entry = s.shards[s.cur_shard].Find(k.hash, k.int_key, k.byte_key);
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
    if (!EncodeJoinKeyInto(row, right_cols_, &key)) { continue;
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

  const SideIndex resident = BuildSideIndex(resident_right, right_cols_);
  std::string probe_scratch;

  while (left_->Next(&row, &position)) {
    if (!EncodeJoinKeyInto(row, left_cols_, &key)) { continue;
}
    const size_t part = partition_of(key);
    if (part == 0 && right_spill[0].Empty()) {
      const KeyRef k =
          KeyOf(row, left_cols_, resident.mode, resident.int_type, &probe_scratch);
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
    if (left_spill[i].Empty() && right_spill[i].Empty()) { continue;
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
  if (kind_ == JoinKind::kSemi) {
    o << "SemiHashJoin: " << ss.str() << "\n" << Indent(indent + 2);
  } else if (kind_ == JoinKind::kAnti) {
    o << "AntiHashJoin: " << ss.str() << "\n" << Indent(indent + 2);
  } else if (kind_ == JoinKind::kLeftOuter) {
    o << "LeftHashJoin: " << ss.str() << "\n" << Indent(indent + 2);
  } else if (kind_ == JoinKind::kRightOuter) {
    o << "RightHashJoin: " << ss.str() << "\n" << Indent(indent + 2);
  } else if (mode_ == HashJoinMode::kHybrid) {
    o << "HybridHashJoin (" << worker_count_ << " workers): " << ss.str()
      << "\n"
      << Indent(indent + 2);
  } else if (pipelined_) {
    o << "PartitionedHashJoin (pipelined, " << worker_count_
      << " workers, build:" << (build_left_side_ ? "left" : "right")
      << "): " << ss.str() << "\n"
      << Indent(indent + 2);
  } else {
    o << "PartitionedHashJoin (" << worker_count_ << " workers): " << ss.str()
      << "\n"
      << Indent(indent + 2);
  }
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_->Dump(o, indent + 2);
}

}  // namespace tinylamb
