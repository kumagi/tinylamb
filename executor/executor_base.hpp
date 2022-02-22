//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_EXECUTOR_BASE_HPP
#define TINYLAMB_EXECUTOR_BASE_HPP

#include "type/row.hpp"

namespace tinylamb {
class Row;

class ExecutorBase {
 public:
  virtual bool Next(Row* dst) = 0;
  virtual ~ExecutorBase() = default;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_BASE_HPP
