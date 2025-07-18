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
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"

namespace tinylamb {
class TransactionContext;

struct QueryData {
 public:
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
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_DATA_HPP
