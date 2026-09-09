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

#ifndef TINYLAMB_EXECUTOR_BASE_HPP
#define TINYLAMB_EXECUTOR_BASE_HPP

#include <iosfwd>
#include <memory>
#include <utility>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"

namespace tinylamb {
struct Row;
struct RowPosition;

class ExecutorBase {
 public:
  ExecutorBase() = default;
  ExecutorBase(const ExecutorBase&) = delete;
  ExecutorBase(ExecutorBase&&) = delete;
  ExecutorBase& operator=(const ExecutorBase&) = delete;
  ExecutorBase& operator=(ExecutorBase&&) = delete;
  virtual ~ExecutorBase() = default;
  virtual bool Next(Row* dst, RowPosition* rp) = 0;
  virtual size_t NextBatch(DataChunk* destination,
                           size_t max_rows = kDefaultVectorSize);
  virtual void Dump(std::ostream& o, int indent) const = 0;
  virtual void Explain(std::ostream& o, int indent) const { Dump(o, indent); }
  friend std::ostream& operator<<(std::ostream& o, const ExecutorBase& e) {
    e.Dump(o, 0);
    return o;
  }

  // Sticky execution error. Next()/NextBatch() return false/0 both at EOF
  // and on failure; callers must inspect GetStatus() after exhaustion to
  // tell the two apart. The first recorded error wins (ARIES-style
  // fail-fast cursor semantics).
  [[nodiscard]] Status GetStatus() const { return status_; }
  [[nodiscard]] bool ok() const { return status_ == Status::kSuccess; }

 protected:
  // Records the first error; returns false so callers can write
  // `return FailWith(...)` inside Next().
  bool FailWith(Status status) const {
    if (status_ == Status::kSuccess) {
      status_ = std::move(status);
    }
    return false;
  }
  // Copies a child's sticky error when the child stops without one of its
  // own rows; returns true if a failure was forwarded (caller returns
  // false).
  bool FailWithChildOf(const ExecutorBase& child) const {
    if (child.GetStatus() != Status::kSuccess) {
      return FailWith(child.GetStatus());
    }
    return false;
  }

 private:
  mutable Status status_{Status::kSuccess};
};

using Executor = std::shared_ptr<ExecutorBase>;

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_BASE_HPP
