/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_JOIN_KIND_HPP
#define TINYLAMB_COMMON_JOIN_KIND_HPP

#include <cstdint>

namespace tinylamb {

// Logical output shape shared by the optimizer and physical join operators.
// This belongs in the common layer so plan code does not depend on executor
// headers.
enum class JoinKind : uint8_t {
  kInner = 0,
  kSemi = 1,
  kAnti = 2,
  kNullAwareAnti = 3,
  kLeftOuter = 4,
  kRightOuter = 5,
  kFullOuter = 6,
};

}  // namespace tinylamb

#endif  // TINYLAMB_COMMON_JOIN_KIND_HPP
