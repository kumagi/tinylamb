import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
/-!
Registers the checked result for `WindowRetention.EvictedValueSetDifference.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_EvictedValueSetDifference
open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

/-!
Refutes `EvictedValueSetDifference`: erasing every occurrence of the evicted value `x`
from the deduplicated set also removes a duplicate copy of `x` that was still inside the
window. With x = 0, middle = [0], entered = 1 the left side is `Perm [1]` while the right
side is `[0, 1]`.
-/

theorem refute : ¬ EvictedValueSetDifference.Goal := by
  intro h
  have key : ¬ ((((0 :: [0] : List Nat).eraseDups.erase 0 ++ [1]).eraseDups).Perm
      (([0] ++ [1] : List Nat)).eraseDups) := by decide
  exact key (h 0 [0] 1)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_EvictedValueSetDifference
