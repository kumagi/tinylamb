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

#ifndef TINYLAMB_HASH_JOIN_HPP
#define TINYLAMB_HASH_JOIN_HPP

#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "executor/executor_base.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class HashJoin : public ExecutorBase {
 public:
  HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
           std::vector<slot_t> right_cols,
           size_t worker_count = std::thread::hardware_concurrency());
  HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
           std::vector<slot_t> right_cols, HashJoinMode mode,
           size_t worker_count = std::thread::hardware_concurrency());
  HashJoin(const HashJoin&) = delete;
  HashJoin(HashJoin&&) = delete;
  HashJoin& operator=(const HashJoin&) = delete;
  HashJoin& operator=(HashJoin&&) = delete;
  ~HashJoin() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] size_t WorkerCount() const { return worker_count_; }
  [[nodiscard]] HashJoinMode Mode() const { return mode_; }

 private:
  void Materialize();
  void MaterializeInMemory();
  void MaterializeHybrid();
  bool EmitNextMatch(Row* dst, RowPosition* rp);
  // Wraps Materialize() so a failure never re-consumes the children (which
  // would duplicate or drop rows on retry).
  void MaterializeOrThrow();

  Executor left_;
  std::vector<slot_t> left_cols_;
  Executor right_;
  std::vector<slot_t> right_cols_;

  HashJoinMode mode_{HashJoinMode::kInMemory};
  size_t worker_count_;
  bool materialized_{false};
  bool materialize_failed_{false};
  bool pipelined_{false};
  bool right_built_{false};
  bool left_exhausted_{false};
  std::vector<Row> right_storage_;
  std::unordered_multimap<std::string, const Row*> right_buckets_;
  Row current_left_;
  RowPosition current_left_pos_;
  std::unordered_multimap<std::string, const Row*>::const_iterator match_iter_;
  std::unordered_multimap<std::string, const Row*>::const_iterator match_end_;
  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  QueryMemoryCharge output_charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_HASH_JOIN_HPP
