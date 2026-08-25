/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_SET_OPERATION_HPP
#define TINYLAMB_COMMON_SET_OPERATION_HPP

#include <cstdint>

namespace tinylamb {

enum class SetOperationKind : uint8_t {
  kUnion,
  kUnionAll,
  kIntersect,
  kIntersectAll,
  kExcept,
  kExceptAll,
};

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_SET_OPERATION_HPP
