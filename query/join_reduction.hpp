/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_JOIN_REDUCTION_HPP
#define TINYLAMB_JOIN_REDUCTION_HPP

namespace tinylamb {

class SelectStatement;

// Null-rejecting WHERE conjuncts make the NULL-padded rows of a LEFT JOIN
// unreachable, so the outer join can be planned as an inner join. Rewrites
// every source whose join_type is kLeft into kInner when the statement's
// WHERE clause rejects its NULL padding (§7.5). Returns true when any join
// kind changed.
bool ReduceOuterJoins(SelectStatement* statement);

}  // namespace tinylamb

#endif  // TINYLAMB_JOIN_REDUCTION_HPP
