/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_hash_join.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/join_kind.hpp"
#include "executor/query_memory.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

uint64_t Fnv1aHash(std::string_view bytes) {
  uint64_t hash = 14695981039346656037ULL;
  for (unsigned char c : bytes) {
    hash ^= static_cast<uint64_t>(c);
    hash *= 1099511628211ULL;
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

void ConcurrentJoinHashTable::Insert(uint64_t hash, std::string key_bytes,
                                     Row row, RowPosition position) {
  const size_t shard_idx = hash % kShardCount;
  auto& shard = shards_[shard_idx];
  std::lock_guard<std::mutex> lock(shard.mutex);
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

std::vector<std::pair<Row, RowPosition>> ConcurrentJoinHashTable::FindMatches(
    uint64_t hash, std::string_view key_bytes) const {
  const size_t shard_idx = hash % kShardCount;
  const auto& shard = shards_[shard_idx];
  std::vector<std::pair<Row, RowPosition>> matches;
  std::lock_guard<std::mutex> lock(shard.mutex);
  const size_t slot = (hash >> 8) & shard.mask;
  size_t curr = shard.head_slots[slot];
  while (curr != kNil) {
    const auto& entry = shard.entries[curr];
    if (entry.hash == hash && entry.key_bytes == key_bytes) {
      matches.emplace_back(entry.row, entry.position);
    }
    curr = entry.next;
  }
  return matches;
}

size_t ConcurrentJoinHashTable::Size() const {
  size_t total = 0;
  for (const auto& shard : shards_) {
    std::lock_guard<std::mutex> lock(shard.mutex);
    total += shard.entries.size();
  }
  return total;
}

size_t ConcurrentJoinHashTable::EstimatedBytes() const {
  size_t bytes = 0;
  for (const auto& shard : shards_) {
    std::lock_guard<std::mutex> lock(shard.mutex);
    bytes += shard.entries.capacity() * sizeof(Entry);
    for (const auto& e : shard.entries) {
      bytes += EstimateRowBytes(e.row) + e.key_bytes.capacity();
    }
  }
  return bytes;
}

SharedBuildParallelHashJoin::SharedBuildParallelHashJoin(
    Executor left, std::vector<slot_t> left_cols, Executor right,
    std::vector<slot_t> right_cols, size_t worker_count, JoinKind kind)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)),
      worker_count_(std::max<size_t>(1, worker_count)),
      kind_(kind),
      shared_hash_table_(2048) {}

std::string SharedBuildParallelHashJoin::MakeKey(
    const Row& row, const std::vector<slot_t>& cols) {
  std::string key;
  for (slot_t col : cols) {
    if (col < row.values_.size()) {
      key += row[col].EncodeMemcomparableFormat();
    }
  }
  return key;
}

uint64_t SharedBuildParallelHashJoin::HashKey(std::string_view key) {
  return Fnv1aHash(key);
}

void SharedBuildParallelHashJoin::BuildSharedHashTable() {
  std::vector<std::pair<Row, RowPosition>> build_tuples;
  Row row;
  RowPosition rp;
  while (right_ && right_->Next(&row, &rp)) {
    build_tuples.emplace_back(std::move(row), rp);
  }

  const size_t total_build = build_tuples.size();
  const size_t num_threads = std::min(worker_count_, std::max<size_t>(1, total_build / 16));

  auto build_worker = [&](size_t thread_id) {
    const size_t chunk_sz = (total_build + num_threads - 1) / num_threads;
    const size_t start = thread_id * chunk_sz;
    const size_t end = std::min(total_build, start + chunk_sz);
    for (size_t i = start; i < end; ++i) {
      const auto& t = build_tuples[i];
      std::string key = MakeKey(t.first, right_cols_);
      const uint64_t hash = HashKey(key);
      shared_hash_table_.Insert(hash, std::move(key), t.first, t.second);
    }
  };

  if (num_threads <= 1) {
    build_worker(0);
  } else {
    std::vector<std::thread> workers;
    workers.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
      workers.emplace_back(build_worker, i);
    }
    for (auto& w : workers) {
      w.join();
    }
  }
}

void SharedBuildParallelHashJoin::ParallelProbe() {
  std::vector<std::pair<Row, RowPosition>> probe_tuples;
  Row row;
  RowPosition rp;
  while (left_ && left_->Next(&row, &rp)) {
    probe_tuples.emplace_back(std::move(row), rp);
  }

  const size_t total_probe = probe_tuples.size();
  const size_t num_threads = std::min(worker_count_, std::max<size_t>(1, total_probe / 16));

  std::vector<std::vector<std::pair<Row, RowPosition>>> thread_outputs(num_threads);

  auto probe_worker = [&](size_t thread_id) {
    const size_t chunk_sz = (total_probe + num_threads - 1) / num_threads;
    const size_t start = thread_id * chunk_sz;
    const size_t end = std::min(total_probe, start + chunk_sz);
    auto& out = thread_outputs[thread_id];

    for (size_t i = start; i < end; ++i) {
      const auto& left_tuple = probe_tuples[i];
      const std::string key = MakeKey(left_tuple.first, left_cols_);
      const uint64_t hash = HashKey(key);
      const auto matches = shared_hash_table_.FindMatches(hash, key);

      switch (kind_) {
        case JoinKind::kInner:
          for (const auto& m : matches) {
            std::vector<Value> joined_values;
            joined_values.reserve(left_tuple.first.values_.size() + m.first.values_.size());
            for (size_t c = 0; c < left_tuple.first.values_.size(); ++c) {
              joined_values.push_back(left_tuple.first[c]);
            }
            for (size_t c = 0; c < m.first.values_.size(); ++c) {
              joined_values.push_back(m.first[c]);
            }
            out.emplace_back(Row(std::move(joined_values)), left_tuple.second);
          }
          break;
        case JoinKind::kSemi:
          if (!matches.empty()) {
            out.emplace_back(left_tuple.first, left_tuple.second);
          }
          break;
        case JoinKind::kAnti:
        case JoinKind::kNullAwareAnti:
          if (matches.empty()) {
            out.emplace_back(left_tuple.first, left_tuple.second);
          }
          break;
        case JoinKind::kLeftOuter:
          if (!matches.empty()) {
            for (const auto& m : matches) {
              std::vector<Value> joined_values;
              joined_values.reserve(left_tuple.first.values_.size() + m.first.values_.size());
              for (size_t c = 0; c < left_tuple.first.values_.size(); ++c) {
                joined_values.push_back(left_tuple.first[c]);
              }
              for (size_t c = 0; c < m.first.values_.size(); ++c) {
                joined_values.push_back(m.first[c]);
              }
              out.emplace_back(Row(std::move(joined_values)), left_tuple.second);
            }
          } else {
            // Null pad right side
            std::vector<Value> joined_values;
            joined_values.reserve(left_tuple.first.values_.size() + right_cols_.size());
            for (size_t c = 0; c < left_tuple.first.values_.size(); ++c) {
              joined_values.push_back(left_tuple.first[c]);
            }
            for (size_t c = 0; c < right_cols_.size(); ++c) {
              joined_values.push_back(Value());
            }
            out.emplace_back(Row(std::move(joined_values)), left_tuple.second);
          }
          break;
        default:
          break;
      }
    }
  };

  if (num_threads <= 1) {
    probe_worker(0);
  } else {
    std::vector<std::thread> workers;
    workers.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
      workers.emplace_back(probe_worker, i);
    }
    for (auto& w : workers) {
      w.join();
    }
  }

  size_t total_out = 0;
  for (const auto& out : thread_outputs) {
    total_out += out.size();
  }
  output_.reserve(total_out);
  for (auto& out : thread_outputs) {
    for (auto& item : out) {
      output_.push_back(std::move(item));
    }
  }

  size_t bytes = output_.size() * (sizeof(RowPosition) + sizeof(Row)) +
                 shared_hash_table_.EstimatedBytes();
  charge_.Add(bytes);
}

void SharedBuildParallelHashJoin::EnsureMaterialized() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  output_.clear();
  output_offset_ = 0;

  BuildSharedHashTable();
  ParallelProbe();
}

void SharedBuildParallelHashJoin::MaterializePipeline() {
  EnsureMaterialized();
}

size_t SharedBuildParallelHashJoin::MaterializedRowCount() const {
  return output_.size();
}

size_t SharedBuildParallelHashJoin::MaterializedBytes() const {
  return charge_.Bytes();
}

bool SharedBuildParallelHashJoin::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  EnsureMaterialized();
  if (output_offset_ >= output_.size()) {
    return false;
  }
  *dst = output_[output_offset_].first;
  if (rp != nullptr) {
    *rp = output_[output_offset_].second;
  }
  ++output_offset_;
  return true;
}

size_t SharedBuildParallelHashJoin::NextBatch(DataChunk* destination,
                                              size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  EnsureMaterialized();
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
    << ", kind=" << static_cast<int>(kind_) << ")";
}

void SharedBuildParallelHashJoin::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
