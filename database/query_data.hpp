//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_QUERY_DATA_HPP
#define TINYLAMB_QUERY_DATA_HPP

#include <string>
#include <vector>

#include "executor/named_expression.hpp"
#include "expression/expression.hpp"

namespace tinylamb {

struct QueryData {
 public:
  std::vector<std::string> from_;
  Expression where_;
  std::vector<NamedExpression> select_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_DATA_HPP
