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
#include <ostream>
#include <string_view>

namespace tinylamb {

static constexpr size_t kPageSize = 1024 * 32;
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
static constexpr size_t kDefaultPagePoolCapacity = (size_t{2} << 30) / kPageSize;

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
  kDeleted,
  kCorrupt,
};

enum class BinaryOperation {
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
    case Status::kIsInfinity:
      return "IsInfinity";
    case Status::kDeleted:
      return "Deleted";
    case Status::kCorrupt:
      return "Corrupt";
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
