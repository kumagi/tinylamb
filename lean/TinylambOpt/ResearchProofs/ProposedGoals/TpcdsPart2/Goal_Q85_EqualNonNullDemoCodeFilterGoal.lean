import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Equal non-NULL demographic codes (TPC-DS Q85)

The `hit` lookup predicate is true precisely when both option codes are the same and
present. This justifies replacing two per-row demographics joins with one bitset lookup
on the equal `(cd1, cd2)` code, preserving the modeled row set.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q85_EqualNonNullDemoCodeFilterGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q85_EqualNonNullDemoCodeFilterGoal := by
  intro x y hhit
  simp only [hit, Bool.and_eq_true, beq_iff_eq] at hhit
  exact ⟨hhit.2, hhit.1⟩

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q85_EqualNonNullDemoCodeFilterGoal
