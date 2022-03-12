//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_EXECUTOR_BASE_HPP
#define TINYLAMB_EXECUTOR_BASE_HPP

#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {
struct Row;
struct RowPosistion;

class ExecutorBase {
 public:
  virtual bool Next(Row* dst, RowPosition* rp) = 0;
  virtual ~ExecutorBase() = default;
  virtual void Dump(std::ostream& o, int indent) const = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_BASE_HPP
