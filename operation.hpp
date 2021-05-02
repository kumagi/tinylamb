//
// Created by kumagi on 2019/09/05.
//

#ifndef PEDASOS_OPERATION_HPP
#define PEDASOS_OPERATION_HPP

#include <memory>

namespace pedasus {

class OpNode {};
class TableScanOp : public OpNode {};
class SelectOp : public OpNode {};
class ProjectOp : public OpNode {};
class NestedLoopJoinOp : public OpNode {};

class Operation {
public:
  std::unique_ptr<OpNode> top_op_;
};

}  // namespace pedasus

#endif // PEDASOS_OPERATION_HPP
