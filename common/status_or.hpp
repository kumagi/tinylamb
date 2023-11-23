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

//
// Created by kumagi on 22/05/08.
//

#ifndef TINYLAMB_STATUS_OR_HPP
#define TINYLAMB_STATUS_OR_HPP

#include <cassert>
#include <optional>

#include "common/constants.hpp"

#define UNLIKELY(x) __builtin_expect((x), 0)

#define ASSIGN_OR_RETURN(type, value, expr)          \
  StatusOr<type> value##_tmp = expr;                 \
  if (value##_tmp.GetStatus() != Status::kSuccess) { \
    return value##_tmp.GetStatus();                  \
  }                                                  \
  type value(value##_tmp.MoveValue())

#define ASSIGN_OR_ASSERT_FAIL(type, value, expr)        \
  StatusOr<type> value##_tmp = expr;                    \
  ASSERT_EQ(value##_tmp.GetStatus(), Status::kSuccess); \
  type value(value##_tmp.Value())

#define COERCE(x)                                              \
  {                                                            \
    Status tmp_status = (x);                                   \
    if (UNLIKELY(tmp_status != Status::kSuccess)) {            \
      LOG(FATAL) << "Crashed: " << #x << " is " << tmp_status; \
      abort();                                                 \
    }                                                          \
  }

#define ASSERT_SUCCESS_AND_EQ(expr, expected)     \
  {                                               \
    auto tmp = expr;                              \
    ASSERT_EQ(tmp.GetStatus(), Status::kSuccess); \
    ASSERT_EQ(tmp.Value(), expected);             \
  }

#define ASSIGN_OR_CRASH(type, value, expr)                \
  StatusOr<type> value##_tmp = expr;                      \
  if (value##_tmp.GetStatus() != Status::kSuccess) {      \
    LOG(FATAL) << "Crashed: " << value##_tmp.GetStatus(); \
  }                                                       \
  assert(value##_tmp.GetStatus() == Status::kSuccess);    \
  type value = value##_tmp.Value()

namespace tinylamb {

// A struct which could have value only when the status is success.
template <typename T>
class StatusOr {
 public:
  // Constructors are intentionally implicit!
  StatusOr(Status s) : status_(s), value_(std::nullopt) {}            // NOLINT
  StatusOr(T v) : status_(Status::kSuccess), value_(std::move(v)) {}  // NOLINT

  [[nodiscard]] bool HasValue() const { return status_ == Status::kSuccess; }
  T& Value() {
    assert(status_ == Status::kSuccess);
    return *value_;
  }
  T&& MoveValue() {
    assert(status_ == Status::kSuccess);
    return std::move(*value_);
  }
  const T& Value() const {
    assert(status_ == Status::kSuccess);
    return *value_;
  }
  [[nodiscard]] Status GetStatus() const { return status_; }
  [[nodiscard]] explicit operator bool() const {
    return status_ == Status::kSuccess;
  }

 private:
  Status status_;
  std::optional<T> value_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_STATUS_OR_HPP
