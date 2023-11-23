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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <string_view>

#ifndef ERROR_CODES_DEFINE
#define ERROR_CODES_DEFINE
#define FATAL 9000
#define ERROR 5000
#define ALERT 4000
#define WARN 3000
#define NOTICE 2500
#define INFO 2000
#define USER 1500
#define DEBUG 1000
#define TRACE 0
#endif  // ERROR_CODE_DEFINE

namespace tinylamb {

static constexpr size_t kPageSize = 1024 * 32;
static constexpr size_t kPageHeaderSize = sizeof(uint64_t) +  // page_id
                                          sizeof(uint64_t) +  // page_lsn
                                          sizeof(uint64_t) +  // rec_lsn
                                          sizeof(uint64_t) +  // page_type
                                          sizeof(uint64_t);   // checksum
static constexpr size_t kPageBodySize = kPageSize - kPageHeaderSize;

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

#define CRASH_IF_FAIL(expr)                                \
  {                                                        \
    Status tmp_status = expr;                              \
    if (tmp_status != Status::kSuccess) assert(!## #expr); \
  }

enum class Status : uint8_t {
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
};

enum class BinaryOperation {
  // Calculations.
  kAdd,
  kSubtract,
  kMultiply,
  kDivide,
  kModulo,

  // Comparisons.
  kEquals,
  kNotEquals,
  kLessThan,
  kLessThanEquals,
  kGreaterThan,
  kGreaterThanEquals,

  // Boolean logics.
  kAnd,
  kOr,
  kXor,
};

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

typedef uint64_t lsn_t;
typedef uint64_t txn_id_t;
typedef uint64_t page_id_t;
typedef uint16_t slot_t;
typedef uint16_t bin_size_t;

static_assert(kPageSize <= std::numeric_limits<slot_t>::max());
static_assert(kPageSize <= std::numeric_limits<bin_size_t>::max());

inline std::string_view ToString(Status s) {
  switch (s) {
    case Status::kUnknown:
      return "Unknown";
    case Status::kSuccess:
      return "Success";
    case Status::kNoSpace:
      return "NoSpace";
    case Status::kDuplicates:
      return "Duplicates";
    case Status::kConflicts:
      return "Conflicts";
    case Status::kUnknownType:
      return "UnknownType";
    case Status::kNotExists:
      return "NotExists";
    case Status::kNotImplemented:
      return "NotImplemented";
    case Status::kTooBigData:
      return "TooBigData";
    case Status::kAmbiguousQuery:
      return "AmbiguousQuery";
    default:
      return "INVALID STATUS";
  }
}

inline std::ostream& operator<<(std::ostream& o, const Status s) {
  o << ToString(s);
  return o;
}

inline std::string Indent(size_t num) {
  return std::string(num, ' ');  // NOLINT
}

}  // namespace tinylamb

#endif  // TINYLAMB_CONSTANTS_HPP
