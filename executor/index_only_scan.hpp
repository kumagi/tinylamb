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

//
// Created by kumagi on 22/06/30.
//

#ifndef TINYLAMB_INDEX_ONLY_SCAN_HPP
#define TINYLAMB_INDEX_ONLY_SCAN_HPP

#include <vector>

#include "executor_base.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"
#include "index/index_scan_iterator.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Index;
class Table;
class Schema;
class Transaction;
class Value;

class IndexOnlyScan : public ExecutorBase {
 public:
  IndexOnlyScan(Transaction& txn, const Table& table, const Index& index,
                const Value& begin, const Value& end, bool ascending,
                Expression where, const Schema& sc,
                TransactionManager::IndexReadGuard index_read_guard = {});
  IndexOnlyScan(Transaction& txn, const Table& table, const Index& index,
                const std::vector<Value>& begin_key,
                const std::vector<Value>& end_key, bool ascending,
                Expression where, const Schema& sc,
                TransactionManager::IndexReadGuard index_read_guard = {});
  IndexOnlyScan(const IndexOnlyScan&) = delete;
  IndexOnlyScan(IndexOnlyScan&&) = delete;
  IndexOnlyScan& operator=(const IndexOnlyScan&) = delete;
  IndexOnlyScan& operator=(IndexOnlyScan&&) = delete;
  ~IndexOnlyScan() override = default;

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  // Called before iteration by a scalar COUNT(DISTINCT key) consumer. Other
  // aggregates still need the multiplicity of every visible index entry.
  bool TryEnableKeyDeduplication();

 private:
  static Schema KeySchema(const Index& idx, const Schema& input_schema);
  static Schema ValueSchema(const Index& idx, const Schema& input_schema);
  static Schema OutputSchema(const Index& idx, const Schema& input_schema);

  TransactionManager::IndexReadGuard index_read_guard_;
  IndexScanIterator iter_;
  Expression cond_;
  bool constant_true_{false};
  // Top-level conjuncts of `cond_`; the filter follows commutative WHERE
  // semantics (executor/detail/scan_filter.hpp) so conjunct order is not
  // observable.
  std::vector<Expression> cond_conjuncts_;
  std::vector<std::optional<BytecodeProgram>> cond_programs_;
  bool key_only_condition_{false};
  std::optional<size_t> condition_key_generation_;
  bool condition_key_passed_{false};
  Schema key_schema_;
  Schema value_schema_;
  Schema output_schema_;
  std::vector<slot_t> include_offsets_;
  bool ascending_{true};
  bool nonunique_keys_{false};
  bool distinct_keys_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_ONLY_SCAN_HPP
