import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow_TerminatedFieldEarlyStop
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Once a zero terminator appears inside an already-buffered chunk `a`,
scanning stops there: extending the buffer with more SQL payload `b` cannot
change the prefix extracted before the terminator. -/
theorem sound : TerminatedFieldEarlyStop.Goal := by
  intro a
  induction a with
  | nil => intro b h; cases h
  | cons x xs ih =>
    intro b h
    rcases (by simpa [List.mem_cons] using h) with h0 | hxs
    · subst h0
      have e0 : ¬ (((0 : Nat) != 0) = true) := by decide
      simp only [beforeZero, List.cons_append, List.takeWhile_cons, if_neg e0]
    · rcases Nat.eq_zero_or_pos x with rfl | hpos
      · have e0 : ¬ (((0 : Nat) != 0) = true) := by decide
        simp only [beforeZero, List.cons_append, List.takeWhile_cons, if_neg e0]
      · have e : (x != 0) = true := by cases x <;> simp_all
        have h2 := ih b hxs
        simp only [beforeZero, List.cons_append, List.takeWhile_cons, if_pos e]
        simp only [beforeZero] at h2
        rw [h2]

end TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow_TerminatedFieldEarlyStop
