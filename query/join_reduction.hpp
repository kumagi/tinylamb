/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_JOIN_REDUCTION_HPP
#define TINYLAMB_JOIN_REDUCTION_HPP

namespace tinylamb {

class SelectStatement;
class TransactionContext;

// Null-rejecting WHERE conjuncts (or null-rejecting HAVING conjuncts when
// every aggregate is a non-COUNT(*) strict fold over the padded side) make the
// NULL-padded rows of an outer join unreachable, so the outer join can be
// planned as an inner join (§7.5, Lean Theorem 4-3). Returns true when any
// join kind changed.
bool ReduceOuterJoins(SelectStatement* statement,
                      TransactionContext* ctx = nullptr);

}  // namespace tinylamb

#endif  // TINYLAMB_JOIN_REDUCTION_HPP
