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
// Created by kumagi on 22/07/14.
//

#ifndef TINYLAMB_INDEX_JOIN_HPP
#define TINYLAMB_INDEX_JOIN_HPP

#include <vector>

#include "common/constants.hpp"
#include "executor/executor_base.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"

namespace tinylamb {
class Table;
class Transaction;

class IndexJoin : public ExecutorBase {
 public:
  IndexJoin(Transaction& txn, Executor left, std::vector<slot_t> left_cols,
            const Table& tbl, const Index& idx, std::vector<slot_t> right_cols);
  IndexJoin(const IndexJoin&) = delete;
  IndexJoin(IndexJoin&&) = delete;
  IndexJoin& operator=(const IndexJoin&) = delete;
  IndexJoin& operator=(IndexJoin&&) = delete;
  ~IndexJoin() override = default;

  bool Next(Row* dst, RowPosition* /* rp */) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  bool Load();

  Transaction& txn_;
  Executor left_;
  std::vector<slot_t> left_cols_;
  const Table& right_;
  const Index& right_idx_;
  Row hold_left_;
  std::unique_ptr<IndexScanIterator> right_it_;
  std::vector<slot_t> right_cols_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_JOIN_HPP
