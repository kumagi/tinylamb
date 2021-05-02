//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_QUERY_DATA_HPP
#define TINYLAMB_QUERY_DATA_HPP

#include <string>
#include <vector>

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
