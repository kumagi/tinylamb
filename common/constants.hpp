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
// ~2 GiB of 32 KiB pages: enough for SF=2 working set without pinning the
// whole database. Tests construct PageManager with a small explicit capacity.
static constexpr size_t kDefaultPagePoolCapacity =
    (size_t{2} << 30) / kPageSize;

#define GET_PAGE_PTR(x) \
  (reinterpret_cast<Page*>(reinterpret_cast<char*>(x) - kPageHeaderSize))
#define GET_PAGE_CONST_PTR(x)                                       \
  (reinterpret_cast<const Page*>(reinterpret_cast<const char*>(x) - \
                                 kPageHeaderSize))
#define GET_CONST_PAGE_PTR(x)                                       \
  (reinterpret_cast<const Page*>(reinterpret_cast<const char*>(x) - \
                                 kPageHeaderSize))

#define RETURN_IF_FAIL(expr)                               \
  {                                                        \
    Status tmp_status = expr;                              \
    if (tmp_status != Status::kSuccess) return tmp_status; \
  }

enum class StatusCode : uint8_t {
  kUnknown,
  kSuccess,
  kNoSpace,
  kConflicts,
  kDuplicates,
  kUnknownType,
  kNotExists,
  kNotImplemented,
  kTooBigData,
  kAmbiguousQuery,
  kIsInfinity,
  kDeleted,
  kCorrupt,
  kIOError,
  kInvalidArgument,
  kRuntimeError,
};

// A status code plus an optional human-readable message. Equality (and every
// `== Status::kXxx` style check) compares the code only; the message is
// diagnostic payload propagated to user-facing error reporting instead of
// C++ exceptions.
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
  static const inline StatusCode kIOError{StatusCode::kIOError};
  static const inline StatusCode kInvalidArgument{StatusCode::kInvalidArgument};
  static const inline StatusCode kRuntimeError{StatusCode::kRuntimeError};

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

// Page id reserved for the MetaPage (allocator state).
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
  }
  return "INVALID STATUS";
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
