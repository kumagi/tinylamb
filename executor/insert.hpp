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

#include <string>
#include <string_view>
#include <utility>

#include "executor/executor_base.hpp"

namespace tinylamb {
class Table;
class Transaction;

class Insert : public ExecutorBase {
 public:
  explicit Insert(Transaction& txn, Table* target, Executor src)
      : txn_(&txn), target_(target), src_(std::move(src)) {}
  ~Insert() override = default;
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Transaction* txn_;
  Table* target_;
  Executor src_;
  bool finished_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INSERT_HPP
