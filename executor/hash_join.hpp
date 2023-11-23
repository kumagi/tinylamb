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
#include <unordered_map>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class HashJoin : public ExecutorBase {
 public:
  HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
           std::vector<slot_t> right_cols);
  HashJoin(const HashJoin&) = delete;
  HashJoin(HashJoin&&) = delete;
  HashJoin& operator=(const HashJoin&) = delete;
  HashJoin& operator=(HashJoin&&) = delete;
  ~HashJoin() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  void BucketConstruct();

  Executor left_;
  std::vector<slot_t> left_cols_;
  Executor right_;
  std::vector<slot_t> right_cols_;

  Row hold_left_;
  std::string left_key_;
  bool bucket_constructed_{false};
  std::unordered_multimap<std::string, Row> right_buckets_;
  std::unordered_multimap<std::string, Row>::const_iterator
      right_buckets_iterator_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_HASH_JOIN_HPP
