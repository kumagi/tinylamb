import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
/-!
Registers the checked result for `WindowRetention.ConsecutiveHaloReplacement.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

/-!
`ConsecutiveHaloReplacement`: at two consecutive spill boundaries it is safe to keep
only the last `w` rows of the *halo-augmented* stream instead of re-deriving the halo
from the full partition history: `tailRows (tailRows a w ++ b) w = tailRows (a ++ b) w`.
Discarded deep history can never re-enter a `w`-bounded ROWS window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_ConsecutiveHaloReplacement
open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.ConsecutiveHaloReplacement

theorem sound : Goal := by
  intro a b w
  simp only [tailRows]
  generalize hA : a.drop (a.length - w) = A
  have hAl : A.length = min w a.length := by rw [← hA]; simp [List.length_drop]; omega
  by_cases hc : w ≤ A.length + b.length
  · -- Split `a` into the dropped prefix `P` and the halo `A`; the RHS index is
  -- `P.length` larger than the LHS index, so `drop_length_add_append` closes it.
    generalize hP : a.take (a.length - w) = P
    have hPa : P ++ A = a := by rw [← hP, ← hA]; exact List.take_append_drop _ _
    rw [← hPa]
    simp only [List.length_append]
    have hidx : P.length + A.length + b.length - w = P.length + (A.length + b.length - w) := by
      omega
    rw [hidx]
    rw [List.append_assoc, List.drop_length_add_append]
  · have hnc : A.length + b.length < w := by omega
    rw [hAl] at hnc
    have halt : a.length < w := by omega
    have hA0 : A = a := by
      rw [← hA, Nat.sub_eq_zero_of_le (Nat.le_of_lt halt), List.drop_zero]
    rw [hA0]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_ConsecutiveHaloReplacement
