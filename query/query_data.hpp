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

#ifndef TINYLAMB_QUERY_DATA_HPP
#define TINYLAMB_QUERY_DATA_HPP

#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"

namespace tinylamb {
class TransactionContext;

struct QueryData {
 public:
  QueryData() = default;
  QueryData(std::vector<std::string> from, Expression where,
            std::vector<NamedExpression> select)
      : from_(std::move(from)),
        where_(std::move(where)),
        select_(std::move(select)) {}

  friend std::ostream& operator<<(std::ostream& o, const QueryData& q) {
    o << "SELECT\n  ";
    for (size_t i = 0; i < q.select_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << q.select_[i];
    }
    o << "\nFROM\n  ";
    for (size_t i = 0; i < q.from_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << q.from_[i];
    }
    o << "\nWHERE\n  " << *q.where_ << ";";
    return o;
  }
  Status Rewrite(TransactionContext& ctx);
  std::vector<std::string> from_;
  Expression where_;
  std::vector<NamedExpression> select_;
  std::unordered_map<std::string, std::string> aliases_;
  // UPDATE/DELETE executors need the physical row position. An index-only
  // scan deliberately has no table row position and must not be selected.
  bool require_row_position_{false};
  std::vector<Expression> order_expressions_;
  std::vector<bool> order_ascending_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_DATA_HPP
