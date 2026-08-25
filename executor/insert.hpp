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

#ifndef TINYLAMB_INSERT_HPP
#define TINYLAMB_INSERT_HPP

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"

namespace tinylamb {
class Table;
class Transaction;

// Mirror of the SQL-level insert modes; kept local so this layer does not
// depend on the statement layer.
enum class InsertExecutionMode {
  kDefault = 0,
  kIgnore = 1,
  kUpsert = 2,
  kReplace = 3,
};

class Insert : public ExecutorBase {
 public:
  explicit Insert(Transaction& txn, Table* target, Executor src)
      : txn_(&txn), target_(target), src_(std::move(src)) {}

  Insert(Transaction& txn, Table* target, Executor src,
         InsertExecutionMode mode, bool enforce_primary_key,
         std::vector<size_t> upsert_offsets, int64_t assert_rows_modified)
      : txn_(&txn),
        target_(target),
        src_(std::move(src)),
        mode_(mode),
        enforce_primary_key_(enforce_primary_key),
        upsert_offsets_(std::move(upsert_offsets)),
        assert_rows_modified_(assert_rows_modified) {}
  ~Insert() override = default;
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Transaction* txn_;
  Table* target_;
  Executor src_;
  InsertExecutionMode mode_{InsertExecutionMode::kDefault};
  bool enforce_primary_key_{false};
  // Destination offsets of explicitly listed columns; used by UPSERT to
  // overwrite only those columns of the conflicting row.
  std::vector<size_t> upsert_offsets_;
  int64_t assert_rows_modified_{-1};
  bool finished_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INSERT_HPP
