/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_JOIN_KIND_HPP
#define TINYLAMB_JOIN_KIND_HPP

#include <cstdint>
#include <string_view>

namespace tinylamb {

// Join kind of a hash join (independent of the physical HashJoinMode):
// kInner: emit every probe/build match pair concatenated.
// kSemi:  emit the probe-side row once when any build row matches.
// kAnti:  emit the probe-side row when no build row matches.
// NULL join keys never match on either side, which is exactly the SQL
// semantics of IN / EXISTS decorrelation under a WHERE clause:
// - `x IN S`: rows with x = NULL or no match are filtered out either way.
// - `NOT EXISTS`: anti join reproduces it exactly.
// `NOT IN` additionally requires both key columns to be NOT NULL before it
// may become a regular anti join (the planner gates this; see optimizer.cpp).
// kNullAwareAnti implements three-valued `NOT IN` behavior directly.
// Outer kinds preserve unmatched rows from the named side(s), padding the
// other side with NULL values. They are kept distinct from HashJoinMode so a
// physical implementation can choose in-memory or hybrid execution
// independently of SQL join semantics.
enum class JoinKind : uint8_t {
  kInner = 0,
  kSemi = 1,
  kAnti = 2,
  kNullAwareAnti = 3,
  kLeftOuter = 4,
  kRightOuter = 5,
  kFullOuter = 6,
};

inline std::string_view JoinKindName(JoinKind kind) {
  switch (kind) {
    case JoinKind::kInner:
      return "HashJoin";
    case JoinKind::kSemi:
      return "SemiHashJoin";
    case JoinKind::kAnti:
      return "AntiHashJoin";
    case JoinKind::kNullAwareAnti:
      return "NullAwareAntiHashJoin";
    case JoinKind::kLeftOuter:
      return "LeftHashJoin";
    case JoinKind::kRightOuter:
      return "RightHashJoin";
    case JoinKind::kFullOuter:
      return "FullHashJoin";
  }
  return "HashJoin";
}

}  // namespace tinylamb

#endif  // TINYLAMB_JOIN_KIND_HPP
