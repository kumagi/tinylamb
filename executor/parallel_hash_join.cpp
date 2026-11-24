/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/platform/process.hpp"
#include "executor/parallel_hash_join.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/parallel_work.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

uint64_t Fnv1aHash(std::string_view bytes) {
  uint64_t hash = 14695981039346656037ULL;
  for (char byte : bytes) {
    hash ^= static_cast<uint64_t>(static_cast<unsigned char>(byte));
    hash *= 1099511628211ULL;
  }
  return hash;
}

}  // namespace

void ConcurrentJoinHashTable::EnableIntKeys(size_t key_parts) {
  if (Size() != 0) {
    return;
  }
  // Refuse instead of clamping. An int-key entry holds at most
  // kMaxIntKeyParts parts, so a clamped width would make both sides compare
  // only the leading four columns and report (1,1,1,1,1) == (1,1,1,1,2).
  // Falling back to the byte-key encoding keeps wider keys exact.
  int_key_parts_ = key_parts <= kMaxIntKeyParts ? key_parts : 0;
}

bool ConcurrentJoinHashTable::IntKeyEquals(const Shard& shard,
                                           size_t entry_index,
                                           const int64_t* key_parts) const {
  const auto& stored = shard.int_keys[entry_index];
  for (size_t i = 0; i < int_key_parts_; ++i) {
    if (stored[i] != key_parts[i]) {
      return false;
    }
  }
  return true;
}

namespace {

// Fnv1a over the raw key-part bytes.  The int-key mode is only chosen when
// every key column carries the same signed-INT64/DATE type, so bit equality
// of the parts agrees with the byte-key equality it replaces (the
// memcomparable images differ only in a type tag that the uniform-type gate
// pins on both sides) and hashing the parts is consistent between build and
// probe.
uint64_t HashIntParts(const int64_t* key_parts, size_t parts) {
  uint64_t hash = 14695981039346656037ULL;
  for (size_t i = 0; i < parts; ++i) {
    const auto bits = static_cast<uint64_t>(key_parts[i]);
    for (int byte = 0; byte < 8; ++byte) {
      hash ^= (bits >> (byte * 8)) & 0xff;
      hash *= 1099511628211ULL;
    }
  }
  return hash;
}

}  // namespace

ConcurrentJoinHashTable::ConcurrentJoinHashTable(size_t expected_entries) {
  const size_t slots_per_shard =
      std::max<size_t>(16, (expected_entries / kShardCount) * 2);
  // Round up to power of two
  size_t capacity = 16;
  while (capacity < slots_per_shard) {
    capacity <<= 1;
  }
  for (auto& shard : shards_) {
    shard.head_slots.assign(capacity, kNil);
    shard.mask = capacity - 1;
  }
}

void ConcurrentJoinHashTable::GrowShard(
    Shard& shard) {  // NOLINT(readability-convert-member-functions-to-static)
  // The initial capacity is derived from a fixed guess (the shared build
  // passes 2048 => 64 slots/shard). Without growth, every extra built row
  // lengthens the probe chains and large joins degrade to O(build*probe/
  // buckets). Grow under the shard's build lock; probes only run after the
  // barrier, so no reader observes a half-rebuilt chain.
  const size_t new_size = shard.head_slots.size() * 2;
  shard.head_slots.assign(new_size, kNil);
  shard.mask = new_size - 1;
  for (size_t i = 0; i < shard.entries.size(); ++i) {
    const size_t slot = (shard.entries[i].hash >> 8) & shard.mask;
    shard.entries[i].next = shard.head_slots[slot];
    shard.head_slots[slot] = i;
  }
}

void ConcurrentJoinHashTable::Insert(uint64_t hash, std::string key_bytes,
                                     Row row, RowPosition position) {
  const size_t shard_idx = hash % kShardCount;
  auto& shard = shards_[shard_idx];
  std::scoped_lock lock(shard.mutex);
  if ((shard.entries.size() + 1) * 10 >= shard.head_slots.size() * 7) {
    GrowShard(shard);
  }
  const size_t entry_idx = shard.entries.size();
  const size_t slot = (hash >> 8) & shard.mask;
  const size_t next = shard.head_slots[slot];
  shard.entries.push_back(Entry{
      .row = std::move(row),
      .position = position,
      .hash = hash,
      .key_bytes = std::move(key_bytes),
      .next = next,
  });
  shard.head_slots[slot] = entry_idx;
}

void ConcurrentJoinHashTable::InsertInt(uint64_t hash, const int64_t* key_parts,
                                        Row row, RowPosition position) {
  const size_t shard_idx = hash % kShardCount;
  auto& shard = shards_[shard_idx];
  std::scoped_lock lock(shard.mutex);
  if ((shard.entries.size() + 1) * 10 >= shard.head_slots.size() * 7) {
    GrowShard(shard);
  }
  const size_t entry_idx = shard.entries.size();
  const size_t slot = (hash >> 8) & shard.mask;
  const size_t next = shard.head_slots[slot];
  std::array<int64_t, kMaxIntKeyParts> stored{};
  for (size_t i = 0; i < int_key_parts_; ++i) {
    stored[i] = key_parts[i];
  }
  shard.int_keys.push_back(stored);
  shard.entries.push_back(Entry{
      .row = std::move(row),
      .position = position,
      .hash = hash,
      .key_bytes = {},
      .next = next,
  });
  shard.head_slots[slot] = entry_idx;
}

size_t ConcurrentJoinHashTable::Size() const {
  size_t total = 0;
  for (const auto& shard : shards_) {
    std::scoped_lock lock(shard.mutex);
    total += shard.entries.size();
  }
  return total;
}

size_t ConcurrentJoinHashTable::EstimatedBytes() const {
  size_t bytes = 0;
  for (const auto& shard : shards_) {
    std::scoped_lock lock(shard.mutex);
    bytes += shard.entries.capacity() * sizeof(Entry);
    bytes += shard.int_keys.capacity() * sizeof(int64_t) * kMaxIntKeyParts;
    for (const auto& e : shard.entries) {
      bytes += EstimateRowBytes(e.row) + e.key_bytes.capacity();
    }
  }
  return bytes;
}

SharedBuildParallelHashJoin::SharedBuildParallelHashJoin(
    Executor left, std::vector<slot_t> left_cols, Executor right,
    std::vector<slot_t> right_cols, size_t worker_count, JoinKind kind,
    size_t right_width)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)),
      worker_count_(std::max<size_t>(1, worker_count)),
      kind_(kind),
      right_width_(right_width),
      shared_hash_table_(2048) {}

std::string SharedBuildParallelHashJoin::MakeKey(
    const Row& row, const std::vector<slot_t>& cols) {
  std::string key;
  for (slot_t col : cols) {
    if (col < row.values_.size()) {
      const Value& value = row[col];
      // PRODUCTION FIX: EncodeMemcomparableFormat() throws on NULL, and
      // MakeKey ran on raw worker threads, so a NULL join key crossed a
      // thread boundary and terminated the whole process.  Encode NULL with
      // a dedicated single-byte tag instead; the value never matches any
      // real key prefix (real encodings start with a value-type byte 1..5).
      if (value.IsNull()) {
        key.push_back('\0');
        continue;
      }
      // Numeric equality matches a nonnegative INT64 against the same
      // UINT64. Their index encodings have different type tags, so normalize
      // the overlapping domain before hashing. High UINT64s keep their tag
      // and cannot collide with negative signed values sharing their bits.
      if (value.type == ValueType::kInt64 && value.IsUnsigned() &&
          value.value.int_value >= 0) {
        key += Value(value.value.int_value).EncodeMemcomparableFormat();
      } else {
        key += value.EncodeMemcomparableFormat();
      }
    }
  }
  return key;
}

bool SharedBuildParallelHashJoin::KeyHasNull(const Row& row,
                                             const std::vector<slot_t>& cols) {
  return std::ranges::any_of(cols, [&row](slot_t col) {
    return col < row.values_.size() && row[col].IsNull();
  });
}

bool SharedBuildParallelHashJoin::KeyHasNaN(const Row& row,
                                            const std::vector<slot_t>& cols) {
  return std::ranges::any_of(cols, [&](slot_t col) {
    if (col >= row.values_.size()) {
      return false;
    }
    const Value& value = row[col];
    if (value.type == ValueType::kDouble) {
      return std::isnan(value.value.double_value);
    }
    if (value.type != ValueType::kArray) {
      return false;
    }
    return std::ranges::any_of(value.ArrayElements(), [](const Value& item) {
      return item.type == ValueType::kDouble &&
             std::isnan(item.value.double_value);
    });
  });
}

uint64_t SharedBuildParallelHashJoin::HashKey(std::string_view key) {
  return Fnv1aHash(key);
}

Status SharedBuildParallelHashJoin::BuildSharedHashTable() {
  // ParallelProbe only emits for these kinds; anything else (right/full
  // outer, mark, single) would drain both inputs and then silently produce
  // an empty success, so reject it up front.
  switch (kind_) {
    case JoinKind::kInner:
    case JoinKind::kSemi:
    case JoinKind::kAnti:
    case JoinKind::kNullAwareAnti:
    case JoinKind::kLeftOuter:
      break;
    default:
      return StatusError(
          StatusCode::kInvalidArgument,
          "SharedBuildParallelHashJoin does not support this join kind");
  }
  std::vector<std::pair<Row, RowPosition>> left_tuples;
  std::vector<std::pair<Row, RowPosition>> right_tuples;
  DataChunk batch;
  while (left_ && left_->NextBatch(&batch) != 0) {
    for (size_t i = 0; i < batch.Size(); ++i) {
      left_tuples.emplace_back(batch.RowAt(i), batch.PositionAt(i));
      charge_.Add(EstimateRowBytes(batch.RowAt(i)) + sizeof(RowPosition));
    }
  }
  if (left_ && left_->GetStatus() != Status::kSuccess) {
    return left_->GetStatus();
  }
  while (right_ && right_->NextBatch(&batch) != 0) {
    for (size_t i = 0; i < batch.Size(); ++i) {
      right_tuples.emplace_back(batch.RowAt(i), batch.PositionAt(i));
      charge_.Add(EstimateRowBytes(batch.RowAt(i)) + sizeof(RowPosition));
    }
  }
  if (right_ && right_->GetStatus() != Status::kSuccess) {
    return right_->GetStatus();
  }

  // Inner joins build the smaller drained side; everything else keeps the
  // right-build orientation its emit paths assume. Both sides are already
  // resident here (this executor materializes both inputs), so the size
  // comparison is free and a planner orientation mistake stops at a slower
  // probe instead of a giant hash table.
  const auto drained_bytes =
      [](const std::vector<std::pair<Row, RowPosition>>& tuples) {
        size_t total = 0;
        for (const auto& item : tuples) {
          total += EstimateRowBytes(item.first);
        }
        return total;
      };
  build_left_ = kind_ == JoinKind::kInner &&
                drained_bytes(left_tuples) < drained_bytes(right_tuples);
  std::vector<std::pair<Row, RowPosition>>& build_tuples =
      build_left_ ? left_tuples : right_tuples;
  probe_tuples_ =
      build_left_ ? std::move(right_tuples) : std::move(left_tuples);
  const std::vector<slot_t>& build_cols =
      build_left_ ? left_cols_ : right_cols_;

  const size_t total_build = build_tuples.size();
  build_empty_ = total_build == 0;
  const size_t num_threads =
      std::min(worker_count_, std::max<size_t>(1, total_build / 16));

  // NULL padding of unmatched left rows must widen to the right row's full
  // COLUMN count (rows are only available to infer it when the build side is
  // non-empty, mirroring the serial HashJoin width contract).
  if (right_width_ == 0 && !build_tuples.empty()) {
    right_width_ = build_tuples.front().first.values_.size();
  }
  // A left-outer join over an empty right side cannot infer the padding
  // width from rows; without an explicit right_width the NULL-padded output
  // would be narrower than the join schema promises (same contract as the
  // serial HashJoin's "outer join requires the empty input width").
  if (kind_ == JoinKind::kLeftOuter && right_width_ == 0 &&
      build_tuples.empty() && !probe_tuples_.empty()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "outer join requires the empty input width");
  }

  // Decide the key mode once, before any insert: fixed-width int keys when
  // every key column of every build row carries the same int-backed type
  // (signed INT64 or DATE; NULL-keyed rows never enter the table and are
  // skipped here).  Cross-type byte keys embed different type tags and can
  // never match under the byte encoding, so raw-part equality is only
  // equivalent to the byte encoding when the type is pinned on both sides —
  // a probe row of any other type takes the no-match path below.
  int build_type = 0;
  bool all_int = total_build > 0;
  for (const auto& t : build_tuples) {
    for (const slot_t col : build_cols) {
      if (col >= t.first.values_.size()) {
        all_int = false;
        break;
      }
      const Value& v = t.first[col];
      if (v.IsNull()) {
        continue;
      }
      const int value_type = (v.type == ValueType::kInt64 && !v.IsUnsigned())
                                 ? 1
                             : (v.type == ValueType::kDate) ? 2
                                                            : 0;
      if (value_type == 0) {
        all_int = false;
        break;
      }
      if (build_type == 0) {
        build_type = value_type;
      } else if (build_type != value_type) {
        all_int = false;
        break;
      }
    }
    if (!all_int) {
      break;
    }
  }
  static const bool int_keys_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_INT_KEY_JOIN") != nullptr;
  // The int-key path stores exactly the key parts it is told, so it is only
  // valid for a key that fits the fixed width AND has the same arity on both
  // sides. A wider (or mismatched) key keeps the memcomparable byte encoding;
  // entering int mode there would drop the trailing columns from both the hash
  // and the equality check, turning distinct keys into false matches.
  // Both sides must have the same arity: the int-key hash and equality
  // only compare min(arity, kMaxIntKeyParts) parts, so asymmetric keys
  // would hash differently and silently lose matches.
  const bool int_key_width_ok =
      build_cols.size() <= ConcurrentJoinHashTable::kMaxIntKeyParts &&
      left_cols_.size() == right_cols_.size() &&
      build_cols.size() == left_cols_.size();
  if (all_int && int_key_width_ok && !int_keys_disabled) {
    int_key_type_ = build_type;
    shared_hash_table_.EnableIntKeys(build_cols.size());
  }

  auto build_worker = [&](size_t thread_id) {
    const size_t chunk_sz = (total_build + num_threads - 1) / num_threads;
    const size_t start = thread_id * chunk_sz;
    const size_t end = std::min(total_build, start + chunk_sz);
    for (size_t i = start; i < end; ++i) {
      const auto& t = build_tuples[i];
      // SQL equality never matches on NULL; a NULL-keyed build row would
      // collide with other NULL-keyed probes through the shared '\0' tag,
      // so it must not enter the hash table at all.
      if (KeyHasNull(t.first, build_cols)) {
        build_has_null_key_ = true;
        continue;
      }
      if (int_key_type_ != 0) {
        std::array<int64_t, 4> parts{};
        for (size_t k = 0; k < build_cols.size() && k < parts.size(); ++k) {
          parts[k] = t.first[build_cols[k]].value.int_value;
        }
        const uint64_t hash =
            HashIntParts(parts.data(), std::min(build_cols.size(), size_t{4}));
        shared_hash_table_.InsertInt(hash, parts.data(), t.first, t.second);
      } else {
        if (KeyHasNaN(t.first, build_cols)) {
          continue;
        }
        std::string key = MakeKey(t.first, build_cols);
        const uint64_t hash = HashKey(key);
        shared_hash_table_.Insert(hash, std::move(key), t.first, t.second);
      }
    }
  };

  RETURN_IF_FAIL(RunParallelTasks(num_threads, build_worker));

  return Status::kSuccess;
}

Status SharedBuildParallelHashJoin::ParallelProbe() {
  // The probe side was drained alongside the build side in
  // BuildSharedHashTable (the orientation is decided there).
  std::vector<std::pair<Row, RowPosition>>& probe_tuples = probe_tuples_;
  const std::vector<slot_t>& probe_cols =
      build_left_ ? right_cols_ : left_cols_;

  const size_t total_probe = probe_tuples.size();
  const size_t num_threads =
      std::min(worker_count_, std::max<size_t>(1, total_probe / 16));

  std::vector<std::vector<std::pair<Row, RowPosition>>> thread_outputs(
      num_threads);

  // Fixed-width int fast path: the build side decided the uniform key type.
  // UINT64 probes in the nonnegative INT64 domain compare numerically with
  // signed build keys. A high UINT64 cannot match a signed key, even when its
  // raw bits look like a negative integer. DATE remains a separate domain.
  const bool int_mode = shared_hash_table_.IntKeyMode();
  const size_t key_parts = std::min(probe_cols.size(), size_t{4});
  auto probe_qualifies = [&](const Row& candidate) {
    return std::ranges::all_of(probe_cols, [&](const slot_t col) {
      if (col >= candidate.values_.size()) {
        return false;
      }
      const Value& v = candidate[col];
      return int_key_type_ == 1 ? (v.type == ValueType::kInt64 &&
                                   (!v.IsUnsigned() || v.value.int_value >= 0))
                                : v.type == ValueType::kDate;
    });
  };

  // Output rows are always left-then-right; when the build side is the left
  // input the lookup result is a left row and the probe tuple is the right
  // one, so the concatenation order flips with it.
  auto emit_combined = [](std::vector<std::pair<Row, RowPosition>>& out,
                          const Row& probe_row, const RowPosition& probe_rp,
                          const Row& build_row, const RowPosition& build_rp,
                          bool build_is_left) {
    const Row& left_row = build_is_left ? build_row : probe_row;
    const Row& right_row = build_is_left ? probe_row : build_row;
    std::vector<Value> joined_values;
    joined_values.reserve(left_row.values_.size() + right_row.values_.size());
    for (size_t c = 0; c < left_row.values_.size(); ++c) {
      joined_values.push_back(left_row[c]);
    }
    for (size_t c = 0; c < right_row.values_.size(); ++c) {
      joined_values.push_back(right_row[c]);
    }
    out.emplace_back(Row(std::move(joined_values)),
                     build_is_left ? build_rp : probe_rp);
  };

  auto probe_worker = [&](size_t thread_id) {
    const size_t chunk_sz = (total_probe + num_threads - 1) / num_threads;
    const size_t start = thread_id * chunk_sz;
    const size_t end = std::min(total_probe, start + chunk_sz);
    auto& out = thread_outputs[thread_id];

    for (size_t i = start; i < end; ++i) {
      const auto& probe_tuple = probe_tuples[i];
      // SQL equality never matches on NULL: a NULL-keyed probe row takes the
      // no-match path for every kind (anti/outer emit it padded or raw).
      const bool probe_has_null = KeyHasNull(probe_tuple.first, probe_cols);
      bool probe_int = false;
      std::array<int64_t, 4> parts{};
      uint64_t int_hash = 0;
      std::string byte_key;
      uint64_t byte_hash = 0;
      if (!probe_has_null) {
        if (int_mode && probe_qualifies(probe_tuple.first)) {
          for (size_t k = 0; k < key_parts; ++k) {
            parts[k] = probe_tuple.first[probe_cols[k]].value.int_value;
          }
          int_hash = HashIntParts(parts.data(), key_parts);
          probe_int = true;
        } else if (!int_mode && !KeyHasNaN(probe_tuple.first, probe_cols)) {
          byte_key = MakeKey(probe_tuple.first, probe_cols);
          byte_hash = HashKey(byte_key);
        }
      }
      // can_match: a lookup worth running.  In byte mode every non-NULL,
      // non-NaN probe row looks up; in int mode only all-integer probe rows
      // can possibly equal an entry.
      const bool can_match = probe_int || !byte_key.empty();

      // Runs the probe lookup, invoking emit per match; true if any match.
      const auto lookup = [&](auto&& emit) {
        bool found = false;
        if (!can_match) {
          return false;
        }
        if (probe_int) {
          shared_hash_table_.FindIntMatches(
              int_hash, parts.data(),
              [&](const Row& m, const RowPosition& position) {
                found = true;
                emit(m, position);
              });
        } else {
          shared_hash_table_.FindMatches(
              byte_hash, byte_key,
              [&](const Row& m, const RowPosition& position) {
                found = true;
                emit(m, position);
              });
        }
        return found;
      };
      // Emits probe+build concatenated in left-then-right schema order.
      auto emit_for = [&](const Row& build_row, const RowPosition& build_rp) {
        emit_combined(out, probe_tuple.first, probe_tuple.second, build_row,
                      build_rp, build_left_);
      };

      if (kind_ == JoinKind::kNullAwareAnti && build_has_null_key_) {
        // NOT IN against a set containing NULL is UNKNOWN unless the probe
        // key matches; UNKNOWN rows are filtered by the surrounding WHERE,
        // so the whole join yields nothing (matches the serial HashJoin).
        continue;
      }
      switch (kind_) {
        case JoinKind::kInner:
          lookup(emit_for);
          break;
        case JoinKind::kSemi:
          if (lookup([](const Row&, const RowPosition&) {})) {
            out.emplace_back(probe_tuple.first, probe_tuple.second);
          }
          break;
        case JoinKind::kAnti:
          // NOT EXISTS decorrelation: a NULL probe key can never match, so
          // it takes the no-match path like the nested-loop/merge joins.
          if (!lookup([](const Row&, const RowPosition&) {})) {
            out.emplace_back(probe_tuple.first, probe_tuple.second);
          }
          break;
        case JoinKind::kNullAwareAnti:
          // NOT IN over an empty set is TRUE even for a NULL probe.
          // Otherwise a NULL probe is UNKNOWN and cannot pass.
          if (build_empty_ ||
              (!probe_has_null &&
               !lookup([](const Row&, const RowPosition&) {}))) {
            out.emplace_back(probe_tuple.first, probe_tuple.second);
          }
          break;
        case JoinKind::kLeftOuter:
          if (!lookup(emit_for)) {
            // Null-pad the right side with its full column count so the
            // output never mixes row widths downstream.
            std::vector<Value> joined_values;
            joined_values.reserve(probe_tuple.first.values_.size() +
                                  right_width_);
            for (size_t c = 0; c < probe_tuple.first.values_.size(); ++c) {
              joined_values.push_back(probe_tuple.first[c]);
            }
            for (size_t c = 0; c < right_width_; ++c) {
              joined_values.emplace_back();
            }
            out.emplace_back(Row(std::move(joined_values)), probe_tuple.second);
          }
          break;
        default:
          break;
      }
    }
  };

  RETURN_IF_FAIL(RunParallelTasks(num_threads, probe_worker));

  size_t total_out = 0;
  for (const auto& out : thread_outputs) {
    total_out += out.size();
  }
  // Account for the materialized output *before* reserving it. Add() is the
  // point that reserves against the global budget (and may trigger spill /
  // eviction): with the charge after reserve(), a build*probe blow-up could
  // allocate the whole contiguous buffer before the budget ever sees it.
  // Saturate instead of wrapping: a build*probe blow-up can push total_out
  // past SIZE_MAX / stride, and a wrapped (tiny) charge would let
  // output_.reserve(total_out) bypass the global memory budget entirely.
  constexpr size_t kRowStride = sizeof(RowPosition) + sizeof(Row);
  constexpr size_t kMaxSize = std::numeric_limits<size_t>::max();
  size_t bytes = kMaxSize;
  if (total_out <= kMaxSize / kRowStride) {
    const size_t rows_bytes = total_out * kRowStride;
    const size_t table_bytes = shared_hash_table_.EstimatedBytes();
    bytes = table_bytes > kMaxSize - rows_bytes ? kMaxSize
                                                : rows_bytes + table_bytes;
  }
  charge_.Add(bytes);
  output_.reserve(total_out);
  for (auto& out : thread_outputs) {
    for (auto& item : out) {
      output_.push_back(std::move(item));
    }
  }

  if (left_ && left_->GetStatus() != Status::kSuccess) {
    return left_->GetStatus();
  }
  return Status::kSuccess;
}

Status SharedBuildParallelHashJoin::EnsureMaterialized() {
  if (materialized_) {
    return materialization_failure_;
  }
  output_.clear();
  output_offset_ = 0;

  Status status = BuildSharedHashTable();
  if (status == Status::kSuccess) {
    status = ParallelProbe();
  }
  if (status != Status::kSuccess) {
    // Latch the failure so a retried Next() keeps reporting it instead of
    // treating the half-built output_ as a complete result.
    materialization_failure_ = status;
  }
  materialized_ = true;
  return status;
}

void SharedBuildParallelHashJoin::MaterializePipeline() {
  if (Status status = EnsureMaterialized(); status != Status::kSuccess) {
    FailWith(status);
  }
}

size_t SharedBuildParallelHashJoin::MaterializedRowCount() const {
  return output_.size();
}

size_t SharedBuildParallelHashJoin::MaterializedBytes() const {
  return charge_.Bytes();
}

size_t SharedBuildParallelHashJoin::NextBatch(DataChunk* destination,
                                              size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  // ExecutorBase contract: NextBatch fills `destination` from scratch, and
  // callers (Selection in particular) trust destination->Size() rather than
  // the returned count. Appending to a reused chunk would re-emit every
  // earlier batch and can exceed max_rows.
  destination->Reset();
  if (Status status = EnsureMaterialized(); status != Status::kSuccess) {
    FailWith(status);
    return 0;
  }
  std::scoped_lock lock(output_mu_);
  if (output_offset_ >= output_.size()) {
    return 0;
  }
  const size_t count = std::min(max_rows, output_.size() - output_offset_);
  for (size_t i = 0; i < count; ++i) {
    destination->Append(output_[output_offset_ + i].first,
                        output_[output_offset_ + i].second);
  }
  output_offset_ += count;
  return count;
}

void SharedBuildParallelHashJoin::Dump(std::ostream& o, int /*indent*/) const {
  o << "SharedBuildParallelHashJoin(workers=" << worker_count_
    << ", kind=" << static_cast<int>(kind_)
    << ", build=" << (build_left_ ? "left" : "right") << ")";
}

void SharedBuildParallelHashJoin::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
