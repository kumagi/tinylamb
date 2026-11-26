import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_DelimiterFreeChunkContinuation
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- A delimiter-free TCP chunk `a` can be consumed incrementally: scanning
`a ++ b` for the zero terminator yields `a` itself prepended to the scan of `b`,
so no bytes of `a` are rescanned when a later fragment arrives. -/
theorem sound : DelimiterFreeChunkContinuation.Goal := by
  intro a
  induction a with
  | nil => intro b _; rfl
  | cons x xs ih =>
    intro b h
    obtain ⟨hx, hxs⟩ : ¬ 0 = x ∧ ¬ 0 ∈ xs := by simpa [List.mem_cons] using h
    rcases Nat.eq_zero_or_pos x with rfl | hpos
    · exact (hx rfl).elim
    · have e : (x != 0) = true := by cases x <;> simp_all
      have h2 := ih b hxs
      simp only [beforeZero, List.cons_append, List.takeWhile_cons, if_pos e]
      simp only [beforeZero] at h2
      rw [h2]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_DelimiterFreeChunkContinuation
