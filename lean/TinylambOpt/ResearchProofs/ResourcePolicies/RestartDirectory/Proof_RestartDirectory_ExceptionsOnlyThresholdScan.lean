import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_RestartDirectory_ExceptionsOnlyThresholdScan
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem sound : ExceptionsOnlyThresholdScan.Goal := by
  intro w xs threshold hth
  unfold flagged
  rw [List.filter_filter]
  congr 1
  funext x
  by_cases hx : threshold ≤ x
  · simp [hx, Nat.le_trans hth hx]
  · simp [hx]
end Nov26.Proof_RestartDirectory_ExceptionsOnlyThresholdScan
