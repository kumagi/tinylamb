import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for CanceledWaiterWakeIsolation. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_CanceledWaiterWakeIsolation

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem sound : CanceledWaiterWakeIsolation.Goal := by
  intro ws d c
  unfold wake
  rw [List.filter_filter, List.filter_filter]
  apply List.filter_congr
  intro w hw
  simp [Bool.and_comm]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_CanceledWaiterWakeIsolation
