//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_PROJECTION_HPP
#define TINYLAMB_PROJECTION_HPP

#include <memory>
#include <vector>

#include "executor_base.hpp"

namespace tinylamb {

class Projection : public ExecutorBase {
 public:
  Projection(std::vector<size_t> offsets, std::unique_ptr<ExecutorBase> src)
      : offsets_(std::move(offsets)), src_(std::move(src)) {}

  bool Next(Row* dst) override;
  ~Projection() override = default;

 private:
  std::vector<size_t> offsets_;
  std::unique_ptr<ExecutorBase> src_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PROJECTION_HPP
