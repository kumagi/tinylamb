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
// Created by kumagi on 22/06/19.
//

#ifndef TINYLAMB_INDEX_SCAN_HPP
#define TINYLAMB_INDEX_SCAN_HPP

#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "index/index_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Index;
class Table;
class Transaction;

class IndexScan : public ExecutorBase {
 public:
  IndexScan(Transaction& txn, const Table& table, const Index& index,
            const Value& begin, const Value& end, bool ascending,
            Expression where, const Schema& sc, bool lock_rows = false,
            bool wait_for_write_intent = true);
  IndexScan(Transaction& txn, const Table& table, const Index& index,
            const std::vector<Value>& begin_key,
            const std::vector<Value>& end_key, bool ascending, Expression where,
            Schema sc, bool lock_rows = false,
            bool wait_for_write_intent = true);
  // Point-union access (Phase 8 IN lists): scans each [begin,end] key range
  // in sequence. Ranges must be sorted and disjoint for ordered output.
  IndexScan(
      Transaction& txn, const Table& table, const Index& index,
      std::vector<std::pair<std::vector<Value>, std::vector<Value>>> ranges,
      bool ascending, Expression where, Schema sc, bool lock_rows = false,
      bool wait_for_write_intent = true);
  IndexScan(const IndexScan&) = delete;
  IndexScan(IndexScan&&) = delete;
  IndexScan& operator=(const IndexScan&) = delete;
  IndexScan& operator=(IndexScan&&) = delete;
  ~IndexScan() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  void OpenRange(const std::vector<Value>& begin_key,
                 const std::vector<Value>& end_key);

  Transaction& txn_;
  const Table& table_;
  const Index& index_;
  bool ascending_;
  bool lock_rows_;
  bool wait_for_write_intent_;
  size_t range_count_{1};
  // Ranges not opened yet (multi-range access only).
  std::vector<std::pair<std::vector<Value>, std::vector<Value>>> pending_;
  size_t pending_offset_{0};
  Iterator iter_;
  Expression cond_;
  // Held by value (not reference): callers often pass a temporary Schema.
  Schema schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCAN_HPP
