import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CommitBatches_CanceledWaiterWakeIsolation
open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches
open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches.CanceledWaiterWakeIsolation

theorem sound : Goal := by
  intro ws d c
  unfold wake
  simp [List.filter_filter]
  congr 1
  funext w
  by_cases hc : w.client != c <;> by_cases hd : w.stop ≤ d <;> simp [hc, hd]
end Nov26.Proof_CommitBatches_CanceledWaiterWakeIsolation
