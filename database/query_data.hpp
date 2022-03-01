//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_QUERY_DATA_HPP
#define TINYLAMB_QUERY_DATA_HPP

#include <string>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

struct QueryData {
 public:
  std::vector<std::string> tables_;
  Expression expression_;
  std::vector<std::string> columns_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_DATA_HPP
