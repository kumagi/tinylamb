import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_TerminatedFieldEarlyStop

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! Early stop after a terminator: once a buffered field prefix contains a zero
byte, scanning the concatenation stops at the same zero as scanning the prefix
alone, even if more SQL payload is already buffered behind it. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TfeProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Induction on the prefix; the first zero short-circuits `takeWhile`. -/
theorem beforeZero_append_of_mem : ∀ (a b : List Nat),
    0 ∈ a → beforeZero (a ++ b) = beforeZero a
  | [], _, h => by simp at h
  | x :: xs, b, h => by
      by_cases hx : x = 0
      · subst x
        simp [beforeZero]
      · have hx0 : ¬ 0 = x := by omega
        have hxs : 0 ∈ xs := by simpa [hx0] using h
        simpa [beforeZero, hx] using beforeZero_append_of_mem xs b hxs

end TfeProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.TerminatedFieldEarlyStop.Goal :=
  TfeProof.beforeZero_append_of_mem
end TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_TerminatedFieldEarlyStop
