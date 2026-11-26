import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_DelimiterFreeChunkContinuation

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! Delimiter-free chunk accumulation: if an already buffered field prefix
contains no zero byte, scanning the concatenation need only continue in the new
chunk; the prefix is copied verbatim, so its bytes are never rescanned. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace DfcProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Induction on the carried prefix: every byte of a zero-free prefix satisfies
`takeWhile` at the same position as in the concatenated stream. -/
theorem beforeZero_append_of_nozero : ∀ (a b : List Nat),
    ¬ 0 ∈ a → beforeZero (a ++ b) = a ++ beforeZero b
  | [], _, _ => by simp [beforeZero]
  | x :: xs, b, h => by
      have hx : x ≠ 0 := by
        intro hx
        exact h (by simp [hx])
      have hxs : ¬ 0 ∈ xs := by
        intro hmem
        exact h (by simp [hmem])
      simpa [beforeZero, hx] using
        congrArg (List.cons x) (beforeZero_append_of_nozero xs b hxs)

end DfcProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.DelimiterFreeChunkContinuation.Goal :=
  DfcProof.beforeZero_append_of_nozero
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_DelimiterFreeChunkContinuation
