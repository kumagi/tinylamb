//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_EXECUTOR_HPP
#define TINYLAMB_EXECUTOR_HPP

#include <iosfwd>
#include <memory>

namespace tinylamb {
struct Row;
struct RowPosition;

class ExecutorBase {
 public:
  virtual bool Next(Row* dst, RowPosition* rp) = 0;
  virtual ~ExecutorBase() = default;
  virtual void Dump(std::ostream& o, int indent) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const ExecutorBase& e) {
    e.Dump(o, 0);
    return o;
  }
};

typedef std::shared_ptr<ExecutorBase> Executor;

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_HPP
