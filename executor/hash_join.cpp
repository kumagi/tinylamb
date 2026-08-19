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
#include <exception>
#include <iterator>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

#include "common/debug.hpp"

namespace tinylamb {

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
                   std::vector<slot_t> right_cols, size_t worker_count)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)),
      worker_count_(std::max<size_t>(1, worker_count)) {}

bool HashJoin::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) Materialize();
  if (output_offset_ == output_.size()) return false;
  *dst = output_[output_offset_].first;
  if (rp) *rp = output_[output_offset_].second;
  ++output_offset_;
  return true;
}

size_t HashJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (!materialized_) Materialize();
  while (output_offset_ < output_.size() &&
         destination->Size() < max_rows) {
    destination->Append(output_[output_offset_].first,
                        output_[output_offset_].second);
    ++output_offset_;
  }
  return destination->Size();
}

void HashJoin::Materialize() {
  using PositionedRow = std::pair<Row, RowPosition>;
  std::vector<PositionedRow> left_rows;
  std::vector<Row> right_rows;
  Row row;
  RowPosition position;
  while (left_->Next(&row, &position)) left_rows.emplace_back(row, position);
  while (right_->Next(&row, nullptr)) {
    right_rows.push_back(row);
  }

  const size_t partitions = std::min(
      worker_count_, std::max<size_t>(1, left_rows.size() + right_rows.size()));
  std::vector<std::vector<PositionedRow>> left_partitions(partitions);
  std::vector<std::vector<Row>> right_partitions(partitions);
  const std::hash<std::string> hash;
  for (PositionedRow& left : left_rows) {
    std::string key =
        left.first.Extract(left_cols_).EncodeMemcomparableFormat();
    left_partitions[hash(key) % partitions].push_back(std::move(left));
  }
  for (Row& right_row : right_rows) {
    std::string key =
        right_row.Extract(right_cols_).EncodeMemcomparableFormat();
    right_partitions[hash(key) % partitions].push_back(std::move(right_row));
  }

  std::vector<std::vector<PositionedRow>> partition_output(partitions);
  std::exception_ptr error;
  std::mutex error_mutex;
  std::vector<std::jthread> workers;
  workers.reserve(partitions);
  for (size_t partition = 0; partition < partitions; ++partition) {
    workers.emplace_back([&, partition] {
      try {
        std::unordered_multimap<std::string, const Row*> buckets;
        buckets.reserve(right_partitions[partition].size());
        for (const Row& right_row : right_partitions[partition]) {
          buckets.emplace(
              right_row.Extract(right_cols_).EncodeMemcomparableFormat(),
              &right_row);
        }
        for (const PositionedRow& left : left_partitions[partition]) {
          const std::string key =
              left.first.Extract(left_cols_).EncodeMemcomparableFormat();
          const auto [begin, end] = buckets.equal_range(key);
          for (auto match = begin; match != end; ++match) {
            partition_output[partition].emplace_back(
                left.first + *match->second, left.second);
          }
        }
      } catch (...) {
        std::scoped_lock lock(error_mutex);
        if (!error) error = std::current_exception();
      }
    });
  }
  workers.clear();
  if (error) std::rethrow_exception(error);
  size_t output_size = 0;
  for (const auto& partition : partition_output) output_size += partition.size();
  output_.reserve(output_size);
  for (auto& partition : partition_output) {
    std::move(partition.begin(), partition.end(), std::back_inserter(output_));
  }
  materialized_ = true;
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
  o << "PartitionedHashJoin (" << worker_count_ << " workers): " << ss.str()
    << "\n" << Indent(indent + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_->Dump(o, indent + 2);
}

}  // namespace tinylamb
