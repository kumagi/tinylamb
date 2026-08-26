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
// Created by kumagi on 22/05/11.
//

#ifndef TINYLAMB_UPDATE_HPP
#define TINYLAMB_UPDATE_HPP

#include "executor_base.hpp"

namespace tinylamb {
class Transaction;
class Table;

class Update : public ExecutorBase {
 public:
  explicit Update(Transaction& txn, Table* target, Executor src)
      : txn_(&txn), target_(target), src_(std::move(src)) {}

  Update(Transaction& txn, Table* target, Executor src,
         int64_t assert_rows_modified)
      : txn_(&txn),
        target_(target),
        src_(std::move(src)),
        assert_rows_modified_(assert_rows_modified) {}

  // Compliance primary-key emulation: when enabled, an UPDATE that assigns
  // the first column must keep keys non-duplicated across the table.
  Update(Transaction& txn, Table* target, Executor src,
         int64_t assert_rows_modified, bool enforce_primary_key)
      : txn_(&txn),
        target_(target),
        src_(std::move(src)),
        assert_rows_modified_(assert_rows_modified),
        enforce_primary_key_(enforce_primary_key) {}

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Transaction* txn_;
  Table* target_;
  Executor src_;
  int64_t assert_rows_modified_{-1};
  bool enforce_primary_key_{false};
  bool finished_{false};
};
}  // namespace tinylamb

#endif  // TINYLAMB_UPDATE_HPP
