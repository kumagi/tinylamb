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

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "index/index_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Index;
class Table;
class Transaction;

class IndexScan : public ExecutorBase {
 public:
  IndexScan(Transaction& txn, const Table& table, const Index& index,
            const Value& begin, const Value& end, bool ascending,
            Expression where, const Schema& sc);
  IndexScan(const IndexScan&) = delete;
  IndexScan(IndexScan&&) = delete;
  IndexScan& operator=(const IndexScan&) = delete;
  IndexScan& operator=(IndexScan&&) = delete;
  ~IndexScan() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Iterator iter_;
  Expression cond_;
  const Schema& schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCAN_HPP
