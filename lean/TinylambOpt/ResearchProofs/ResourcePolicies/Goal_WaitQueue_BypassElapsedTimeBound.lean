import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue
/-!
Registers the checked result for `WaitQueue.BypassElapsedTimeBound.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_BypassElapsedTimeBound
open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

/-!
Refutes `BypassElapsedTimeBound`: appending a newcomer to the cohort can strictly
increase the cohort's maximum elapsed time, so the newcomer's time is not bounded by
the old maximum. Counterexample: old = [1], newcomer = 2.
-/

theorem refute : ¬ BypassElapsedTimeBound.Goal := by
  intro h
  have key : ¬ (([1] : List Nat) ++ [2]).max?.getD 0 ≤ ([1] : List Nat).max?.getD 0 := by decide
  exact key (h [1] 2 (by decide))

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_BypassElapsedTimeBound
