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

#ifndef TINYLAMB_CONSTANTS_HPP
#define TINYLAMB_CONSTANTS_HPP

#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

namespace tinylamb {

static constexpr size_t kPageSize = size_t{1024} * 32;
static constexpr size_t kPageHeaderSize = sizeof(uint32_t) +  // magic
                                          sizeof(uint32_t) +  // version
                                          sizeof(uint64_t) +  // page_id
                                          sizeof(uint64_t) +  // page_lsn
                                          sizeof(uint64_t) +  // rec_lsn
                                          sizeof(uint64_t) +  // page_type
                                          sizeof(uint64_t);   // checksum
static constexpr size_t kPageBodySize = kPageSize - kPageHeaderSize;

#define TINYLAMB_DETAIL_CONCAT_IMPL(a, b) a##b
#define TINYLAMB_DETAIL_CONCAT(a, b) TINYLAMB_DETAIL_CONCAT_IMPL(a, b)

// Unlike ASSIGN_OR_* (which deliberately declares `value` in the caller's
// scope), these macros introduce no caller-visible name, so they are proper
// single statements: safe as a braceless if/else body.
#define RETURN_IF_FAIL(expr)                                                 \
  do {                                                                       \
    ::tinylamb::Status TINYLAMB_DETAIL_CONCAT(tinylamb_tmp_status_, __LINE__) = (expr);  \
    if (TINYLAMB_DETAIL_CONCAT(tinylamb_tmp_status_, __LINE__) !=            \
        ::tinylamb::Status::kSuccess) {                                     \
      return TINYLAMB_DETAIL_CONCAT(tinylamb_tmp_status_, __LINE__);         \
    }                                                                        \
  } while (false)

enum class StatusCode : uint8_t {
  kUnknown = 0,
  kSuccess = 1,
  kNoSpace = 2,
  kConflicts = 3,
  kDuplicates = 4,
  kUnknownType = 5,
  kNotExists = 6,
  kNotImplemented = 7,
  kTooBigData = 8,
  kAmbiguousQuery = 9,
  kIsInfinity = 10,
  kDeleted = 11,
  kCorrupt = 12,
  kCorruption = kCorrupt,
  kIOError = 13,
  kInvalidArgument = 14,
  kRuntimeError = 15,
  kMemoryLimitExceeded = 16,
  // Statement deadline (TINYLAMB_STATEMENT_TIMEOUT_MS) expired mid-run.
  // Distinct from kRuntimeError so clients can tell a cancel/timeout from
  // a server fault (PostgreSQL SQLSTATE 57014 query_canceled).
  kQueryCanceled = 17,
  // Iteration bound, not a real status: every code above must precede this
  // sentinel so status_or_test can walk the whole declared range and fail
  // when a new enumerator forgets its ToString case. Untrusted 1-byte status
  // values must be validated by the decoder (ToString returns
  // "INVALID STATUS" for the sentinel); it is never produced by the engine.
  kStatusCodeCount = 18,
};

// A status code plus an optional human-readable message. Equality (and every
// `== Status::kXxx` style check) compares the code only; the message is
// diagnostic payload propagated to user-facing error reporting instead of
// C++ exceptions. Comparing the message too would make every
// `status == Status::kSuccess`-style check fail for statuses carrying a
// diagnostic, so the message is deliberately compared with GetMessage()
// explicitly where that distinction matters.
class Status {
 public:
  Status() = default;
  Status(StatusCode code) : code_(code) {}  // NOLINT(runtime/explicit)
  Status(StatusCode code, std::string message)
      : code_(code),
        message_(std::make_shared<const std::string>(std::move(message))) {}

  // Named constants keep the historical `Status::kXxx` spelling.
  static const inline StatusCode kUnknown{StatusCode::kUnknown};
  static const inline StatusCode kSuccess{StatusCode::kSuccess};
  static const inline StatusCode kNoSpace{StatusCode::kNoSpace};
  static const inline StatusCode kConflicts{StatusCode::kConflicts};
  static const inline StatusCode kDuplicates{StatusCode::kDuplicates};
  static const inline StatusCode kUnknownType{StatusCode::kUnknownType};
  static const inline StatusCode kNotExists{StatusCode::kNotExists};
  static const inline StatusCode kNotImplemented{StatusCode::kNotImplemented};
  static const inline StatusCode kTooBigData{StatusCode::kTooBigData};
  static const inline StatusCode kAmbiguousQuery{StatusCode::kAmbiguousQuery};
  static const inline StatusCode kIsInfinity{StatusCode::kIsInfinity};
  static const inline StatusCode kDeleted{StatusCode::kDeleted};
  static const inline StatusCode kCorrupt{StatusCode::kCorrupt};
  static const inline StatusCode kCorruption{StatusCode::kCorrupt};
  static const inline StatusCode kIOError{StatusCode::kIOError};
  static const inline StatusCode kInvalidArgument{StatusCode::kInvalidArgument};
  static const inline StatusCode kRuntimeError{StatusCode::kRuntimeError};
  static const inline StatusCode kMemoryLimitExceeded{
      StatusCode::kMemoryLimitExceeded};
  static const inline StatusCode kQueryCanceled{StatusCode::kQueryCanceled};

  [[nodiscard]] StatusCode GetCode() const { return code_; }
  [[nodiscard]] bool ok() const { return code_ == StatusCode::kSuccess; }
  [[nodiscard]] const std::string& GetMessage() const {
    static const std::string kEmpty;
    return message_ != nullptr ? *message_ : kEmpty;
  }

  friend bool operator==(const Status& lhs, const Status& rhs) {
    return lhs.code_ == rhs.code_;
  }
  friend bool operator!=(const Status& lhs, const Status& rhs) {
    return !(lhs == rhs);
  }

 private:
  StatusCode code_{StatusCode::kUnknown};
  std::shared_ptr<const std::string> message_;
};

// Convenience factory for error paths: StatusError(StatusCode::kCorrupt,
// "bad header").
inline Status StatusError(StatusCode code, std::string message) {
  return {code, std::move(message)};
}

enum class BinaryOperation : uint8_t {
  // Calculations.
  kAdd,
  kSubtract,
  kMultiply,
  kDivide,
  kModulo,
  kShiftLeft,
  kShiftRight,

  // Comparisons.
  kEquals,
  kNotEquals,
  kLessThan,
  kLessThanEquals,
  kGreaterThan,
  kGreaterThanEquals,
  kLike,
  kNotLike,
  kIsDistinctFrom,
  kIsNotDistinctFrom,

  // Boolean logics.
  kAnd,
  kOr,
  kXor,
};

inline std::string_view ToString(BinaryOperation op) {
  switch (op) {
    case BinaryOperation::kAdd:
      return "+";
    case BinaryOperation::kSubtract:
      return "-";
    case BinaryOperation::kMultiply:
      return "*";
    case BinaryOperation::kDivide:
      return "/";
    case BinaryOperation::kModulo:
      return "%";
    case BinaryOperation::kShiftLeft:
      return "<<";
    case BinaryOperation::kShiftRight:
      return ">>";
    case BinaryOperation::kEquals:
      return "=";
    case BinaryOperation::kNotEquals:
      return "!=";
    case BinaryOperation::kLessThan:
      return "<";
    case BinaryOperation::kLessThanEquals:
      return "<=";
    case BinaryOperation::kGreaterThan:
      return ">";
    case BinaryOperation::kGreaterThanEquals:
      return ">=";
    case BinaryOperation::kLike:
      return "LIKE";
    case BinaryOperation::kNotLike:
      return "NOT LIKE";
    case BinaryOperation::kIsDistinctFrom:
      return "IS DISTINCT FROM";
    case BinaryOperation::kIsNotDistinctFrom:
      return "IS NOT DISTINCT FROM";
    case BinaryOperation::kAnd:
      return "AND";
    case BinaryOperation::kOr:
      return "OR";
    case BinaryOperation::kXor:
      return "XOR";
  }
  return "INVALID";
}

inline bool IsComparison(enum BinaryOperation op) {
  switch (op) {
    case BinaryOperation::kEquals:
    case BinaryOperation::kNotEquals:
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      return true;

    default:
      return false;
  }
}

using lsn_t = uint64_t;
using txn_id_t = uint64_t;
using page_id_t = uint64_t;
using slot_t = uint16_t;
using bin_size_t = uint16_t;
constexpr uint64_t kMaxSerializedVectorElements = uint64_t{1} << 20;

// Page id reserved for the MetaPage (allocator state). Currently unused; the
// allocator state lives in PageManager itself.
constexpr page_id_t kMetaPageId = 0;

static_assert(kPageSize <= std::numeric_limits<slot_t>::max());
static_assert(kPageSize <= std::numeric_limits<bin_size_t>::max());

inline std::string_view ToString(StatusCode s) {
  switch (s) {
    case StatusCode::kUnknown:
      return "Unknown";
    case StatusCode::kSuccess:
      return "Success";
    case StatusCode::kNoSpace:
      return "NoSpace";
    case StatusCode::kDuplicates:
      return "Duplicates";
    case StatusCode::kConflicts:
      return "Conflicts";
    case StatusCode::kUnknownType:
      return "UnknownType";
    case StatusCode::kNotExists:
      return "NotExists";
    case StatusCode::kNotImplemented:
      return "NotImplemented";
    case StatusCode::kTooBigData:
      return "TooBigData";
    case StatusCode::kAmbiguousQuery:
      return "AmbiguousQuery";
    case StatusCode::kIsInfinity:
      return "IsInfinity";
    case StatusCode::kDeleted:
      return "Deleted";
    case StatusCode::kCorrupt:
      return "Corrupt";
    case StatusCode::kIOError:
      return "IOError";
    case StatusCode::kInvalidArgument:
      return "InvalidArgument";
    case StatusCode::kRuntimeError:
      return "RuntimeError";
    case StatusCode::kMemoryLimitExceeded:
      return "MemoryLimitExceeded";
    case StatusCode::kQueryCanceled:
      return "QueryCanceled";
    case StatusCode::kStatusCodeCount:
      return "INVALID STATUS";
  }
  return "INVALID STATUS";
}

// PostgreSQL SQLSTATE mapping for statement-execution errors (ADR 0012).
[[nodiscard]] inline std::string_view SqlStateForStatus(
    const Status& s, std::string_view fallback = "XX000") noexcept {
  switch (s.GetCode()) {
    case StatusCode::kMemoryLimitExceeded:
      return "53200";
    case StatusCode::kNoSpace:
      return "53100";
    case StatusCode::kConflicts:
      return "40001";  // serialization_failure (write-intent conflict/wound)
    case StatusCode::kDuplicates:
      return "23505";  // unique_violation
    case StatusCode::kQueryCanceled:
      return "57014";  // query_canceled (statement timeout)
    case StatusCode::kNotImplemented:
      return "0A000";  // feature_not_supported
    case StatusCode::kInvalidArgument:
      // The overloaded "bad SQL semantics" status: identical at Prepare and
      // at execution time (P2838 removed the per-call-site split).
      return "42601";  // syntax_error
    case StatusCode::kNotExists:
      return "42P01";  // undefined_table (dominant use: missing relation)
    case StatusCode::kAmbiguousQuery:
      return "42702";  // ambiguous_column
    case StatusCode::kUnknownType:
      return "42704";  // undefined_object
    case StatusCode::kTooBigData:
      // P2842: a row / aggregate value that exceeds the operator budget is
      // a rejected program limit (ADR 0012 defines kTooBigData apart from
      // kMemoryLimitExceeded/kNoSpace), not a SQL syntax error and not
      // silent truncation. PostgreSQL's program_limit_exceeded is 54000.
      return "54000";
    case StatusCode::kIsInfinity:
      return "22003";  // numeric_value_out_of_range
    case StatusCode::kRuntimeError:
    case StatusCode::kIOError:
    case StatusCode::kCorrupt:
      return "XX000";  // internal_error: server-side fault, not retryable
    default:
      return fallback;
  }
}

inline std::string ToString(const Status& s) {
  std::string result(ToString(s.GetCode()));
  if (!s.GetMessage().empty()) {
    result += ": ";
    result += s.GetMessage();
  }
  return result;
}

inline std::ostream& operator<<(std::ostream& o, const StatusCode& s) {
  o << ToString(s);
  return o;
}

inline std::ostream& operator<<(std::ostream& o, const Status& s) {
  o << ToString(s);
  return o;
}

inline std::string Indent(size_t num) {
  return std::string(num, ' ');  // NOLINT
}

}  // namespace tinylamb

#endif  // TINYLAMB_CONSTANTS_HPP
