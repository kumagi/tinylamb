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
#include <cstdlib>
#include <optional>
#include <ostream>
#include <type_traits>

#include "common/constants.hpp"
#include "common/log_message.hpp"

#define UNLIKELY(x) __builtin_expect((x), 0)

// gtest-free counterpart of ASSERT_EQ for the assertion macros below. This
// production header is included by servers/benchmarks that do not link gtest,
// and gtest's ASSERT_EQ also cannot be used from non-void callers.
#define TINYLAMB_STATUS_OR_ASSERT_EQ(a, b) \
  ::tinylamb::detail::StatusOrAssertEq((a), (b), #a, #b)

// NOTE: Unlike COERCE/ASSERT_SUCCESS_AND_EQ, the ASSIGN_OR_* family is
// deliberately *not* `do { ... } while (0)`: each macro declares `value` in
// the caller's scope so it can be used by the following statements, and a
// do-while would scope it away. As multi-statement macros they are not single
// statements -- always brace them when used as an if/else/for/while body
// (`if (c) { ASSIGN_OR_RETURN(...); }`), otherwise the caller's `else` no
// longer matches its `if` and compilation fails instead of silently rebinding.
// For the same reason `value##_tmp` lives in the caller's scope: do not reuse
// the same `value` name twice in one scope (e.g. two calls in a loop body) --
// pick a distinct name, or open a nested scope, for the second call.
#define ASSIGN_OR_RETURN(type, value, expr)          \
  StatusOr<type> value##_tmp = expr;                 \
  if (value##_tmp.GetStatus() != Status::kSuccess) { \
    return value##_tmp.GetStatus();                  \
  }                                                  \
  type value(value##_tmp.MoveValue())

#define ASSIGN_OR_ASSERT_FAIL(type, value, expr)                           \
  StatusOr<type> value##_tmp = expr;                                       \
  TINYLAMB_STATUS_OR_ASSERT_EQ(value##_tmp.GetStatus(), Status::kSuccess); \
  type value(value##_tmp.MoveValue())

// Like ASSIGN_OR_ASSERT_FAIL, but binds `value` as a const reference into the
// temporary StatusOr so large values (Table, Row, TableStatistics, ...) are
// not copied. Use only when `value` is never mutated afterwards.
#define ASSIGN_OR_ASSERT_FAIL_CONST(type, value, expr)        \
  auto value##_const_tmp = expr;                              \
  TINYLAMB_STATUS_OR_ASSERT_EQ(value##_const_tmp.GetStatus(), \
                               Status::kSuccess);             \
  const type& value(value##_const_tmp.Value())

#define COERCE(x)                                              \
  {                                                            \
    const Status& tmp_status = (x);                            \
    if (UNLIKELY(tmp_status != Status::kSuccess)) {            \
      LOG(FATAL) << "Crashed: " << #x << " is " << tmp_status; \
      abort();                                                 \
    }                                                          \
  }

#define ASSERT_SUCCESS_AND_EQ(expr, expected)                        \
  {                                                                  \
    const auto& tmp = expr;                                          \
    TINYLAMB_STATUS_OR_ASSERT_EQ(tmp.GetStatus(), Status::kSuccess); \
    TINYLAMB_STATUS_OR_ASSERT_EQ(tmp.Value(), expected);             \
  }

#define ASSIGN_OR_CRASH(type, value, expr)                     \
  StatusOr<type> value##_tmp = expr;                           \
  if (UNLIKELY(value##_tmp.GetStatus() != Status::kSuccess)) { \
    LOG(FATAL) << "Crashed: " << value##_tmp.GetStatus();      \
    abort();                                                   \
  }                                                            \
  type value = value##_tmp.MoveValue()

// Like ASSIGN_OR_CRASH, but binds `value` as a const reference into the
// temporary StatusOr so the value is not copied. Use only when `value` is
// never mutated afterwards.
#define ASSIGN_OR_CRASH_CONST(type, value, expr)                     \
  auto value##_const_tmp = expr;                                     \
  if (UNLIKELY(value##_const_tmp.GetStatus() != Status::kSuccess)) { \
    LOG(FATAL) << "Crashed: " << value##_const_tmp.GetStatus();      \
    abort();                                                         \
  }                                                                  \
  /* NOLINTNEXTLINE(bugprone-macro-parentheses) */                   \
  const type& value = value##_const_tmp.Value()

namespace tinylamb {

namespace detail {
// Cold path of StatusOr value access. Outlined so the hot check in
// Value()/MoveValue() stays one predictable branch that inlines everywhere;
// the LOG(FATAL) machinery would otherwise keep the whole accessor out of
// line on the row-decode hot path.
[[noreturn]] inline void StatusOrNoValue(const Status& status) {
  LOG(FATAL) << "StatusOr has no value: " << status;
  std::abort();
}

// Self-contained assertion for the ASSERT_* convenience macros. Unlike the
// gtest ASSERT_EQ it used to expand to, this works in binaries that do not
// link gtest and is not restricted to void return contexts. Unlike assert()
// it stays active in NDEBUG builds. It does not print the values, so no
// operator<< is required from the compared types.
template <typename A, typename B>
inline void StatusOrAssertEq(const A& a, const B& b, const char* lhs,
                             const char* rhs) {
  if (UNLIKELY(!(a == b))) {
    LOG(FATAL) << "Assertion failed: " << lhs << " == " << rhs;
    std::abort();
  }
}
}  // namespace detail

// A struct which could have value only when the status is success.
template <typename T>
class StatusOr {
 public:
  // Constructors are intentionally implicit!
  StatusOr(Status s) : status_(s), value_(std::nullopt) {}            // NOLINT
  StatusOr(StatusCode code) : status_(code), value_(std::nullopt) {}  // NOLINT
  StatusOr(T v)
      : status_(Status::kSuccess),
        value_(std::in_place, std::move(v)) {}  // NOLINT
  // Implicit conversion from anything convertible to T (e.g. string literals
  // into StatusOr<std::string>) so `return "abc";` keeps working.
  template <typename U>
    requires(std::is_convertible_v<U &&, T> &&
             !std::is_same_v<std::decay_t<U>, StatusOr<T>> &&
             !std::is_same_v<std::decay_t<U>, T> &&
             !std::is_same_v<std::decay_t<U>, Status> &&
             !std::is_same_v<std::decay_t<U>, StatusCode>)
  StatusOr(U&& v)  // NOLINT
      : status_(Status::kSuccess), value_(std::in_place, std::forward<U>(v)) {}

  // Copy/move propagate consumed_ like any other member, so a consumed
  // StatusOr never becomes readable again through a copy: the optional still
  // holds the moved-from payload, and only the consumed_ flag keeps Value()
  // and HasValue() honest. Declaring all four explicitly documents that
  // invariant (and keeps the move operations alive despite the copy ones).
  StatusOr(const StatusOr&) = default;
  StatusOr(StatusOr&&) = default;
  StatusOr& operator=(const StatusOr&) = default;
  StatusOr& operator=(StatusOr&&) = default;

  [[nodiscard]] bool HasValue() const {
    // A success Status with no payload (e.g. `StatusOr<T> x =
    // Status::kSuccess`) must not claim a value: Value()/MoveValue() would
    // abort.
    return !consumed_ && status_ == Status::kSuccess && value_.has_value();
  }
  // Callers must check HasValue() first: accessing the value of a failed
  // StatusOr is a contract violation and aborts (no exceptions).
  [[nodiscard]] T& Value() {
    if (UNLIKELY(consumed_ || status_ != Status::kSuccess ||
                 !value_.has_value())) {
      detail::StatusOrNoValue(ConsumedStatus());
    }
    return *value_;
  }
  // Moves the value out and consumes the StatusOr: a second MoveValue() (or
  // Value()) on the same object aborts. Track consumption separately from
  // status_ so GetStatus() keeps the original error (e.g. kCorrupt) instead
  // of overwriting it with kUnknown, which is indistinguishable from a
  // genuine failure.
  [[nodiscard]] T&& MoveValue() {
    if (UNLIKELY(consumed_ || status_ != Status::kSuccess ||
                 !value_.has_value())) {
      detail::StatusOrNoValue(ConsumedStatus());
    }
    consumed_ = true;
    return std::move(*value_);
  }
  [[nodiscard]] const T& Value() const {
    if (UNLIKELY(consumed_ || status_ != Status::kSuccess ||
                 !value_.has_value())) {
      detail::StatusOrNoValue(ConsumedStatus());
    }
    return *value_;
  }
  [[nodiscard]] Status GetStatus() const { return status_; }
  [[nodiscard]] explicit operator bool() const { return HasValue(); }

  friend std::ostream& operator<<(std::ostream& o, const StatusOr<T>& so) {
    if (so.HasValue()) {
      o << so.Value();
    } else if (so.GetStatus() == Status::kSuccess) {
      o << "<empty>";
    } else {
      o << "<error: " << so.GetStatus() << ">";
    }
    return o;
  }

 private:
  [[nodiscard]] Status ConsumedStatus() const {
    return consumed_ ? Status(StatusCode::kUnknown) : status_;
  }

  Status status_;
  std::optional<T> value_;
  bool consumed_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_STATUS_OR_HPP
