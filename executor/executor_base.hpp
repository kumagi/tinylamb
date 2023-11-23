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
  virtual void Dump(std::ostream& o, int indent) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const ExecutorBase& e) {
    e.Dump(o, 0);
    return o;
  }
};

typedef std::shared_ptr<ExecutorBase> Executor;

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_BASE_HPP
