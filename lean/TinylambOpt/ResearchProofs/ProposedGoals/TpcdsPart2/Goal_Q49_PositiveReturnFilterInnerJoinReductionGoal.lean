import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Null-rejecting return filter after a LEFT OUTER JOIN (TPC-DS Q49)

The modeled `return_quantity > 0` predicate rejects NULL-extended outer-join rows,
so counting qualifying `some q` rows is exactly the inner-join count after `filterMap id`.
The SQL executor may therefore turn the outer join into an inner join without changing
the modeled result; no claim is made about physical join algorithms.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q49_PositiveReturnFilterInnerJoinReductionGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q49_PositiveReturnFilterInnerJoinReductionGoal := by
  intro rows
  induction rows with
  | nil => rfl
  | cons r rows ih =>
    cases r with
    | none => simpa using ih
    | some q =>
      by_cases h : 0 < q <;> simp [h, ih]

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q49_PositiveReturnFilterInnerJoinReductionGoal
