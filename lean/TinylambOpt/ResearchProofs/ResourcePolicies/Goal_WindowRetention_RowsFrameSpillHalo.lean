import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
/-!
Registers the checked result for `WindowRetention.RowsFrameSpillHalo.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

/-!
`RowsFrameSpillHalo`: when a partition is streamed as two morsels `a ++ b`, every
bounded ROWS frame ending inside `b` is unchanged if the already-emitted prefix `a`
is replaced by its last `w` rows (the halo). SQL execution may therefore drop all
older rows of `a` at the spill boundary without changing any output.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_RowsFrameSpillHalo
open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.RowsFrameSpillHalo

theorem sound : Goal := by
  intro a b w j hj
  simp only [frame, tailRows]
  generalize hA : a.drop (a.length - w) = A
  by_cases hw : w ≤ a.length
  · -- The halo is exactly `w` rows long, so both takes cut inside `b`.
    have hAl : A.length = w := by rw [← hA]; simp [List.length_drop]; omega
    have htL : (a ++ b).take (a.length + j + 1) = a ++ b.take (j + 1) := by
      rw [show a.length + j + 1 = a.length + (j + 1) by omega, List.take_length_add_append]
    have htR : (A ++ b).take (A.length + j + 1) = A ++ b.take (j + 1) := by
      rw [show A.length + j + 1 = A.length + (j + 1) by omega, List.take_length_add_append]
    rw [htL, htR, hAl]
    have h1 : a.length + j - w = a.length - w + j := by omega
    have h2 : w + j - w = j := by omega
    rw [h1, h2]
    -- Goal: (a ++ b.take (j+1)).drop (a.length - w + j) = (A ++ b.take (j+1)).drop j
    by_cases hjw : j ≤ w
    · -- The drop stays inside the prefix on both sides.
      rw [List.drop_append_of_le_length (show a.length - w + j ≤ a.length by omega)]
      rw [List.drop_append_of_le_length (show j ≤ A.length by omega)]
      rw [← hA, List.drop_drop]
    · -- The drop passes the prefix on both sides, leaving `b.take (j+1)` trimmed by `j - w`.
      have hjw' : w < j := by omega
      simp only [List.drop_append]
      rw [List.drop_eq_nil_of_le (show a.length ≤ a.length - w + j by omega)]
      rw [List.drop_eq_nil_of_le (show A.length ≤ j by omega)]
      rw [List.nil_append, List.nil_append]
      congr 1
      omega
  · -- `w` exceeds `a`, so the halo is all of `a` and the statement is reflexivity.
    have halt : a.length < w := by omega
    have hA0 : A = a := by
      rw [← hA, Nat.sub_eq_zero_of_le (Nat.le_of_lt halt), List.drop_zero]
    rw [hA0]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_RowsFrameSpillHalo
