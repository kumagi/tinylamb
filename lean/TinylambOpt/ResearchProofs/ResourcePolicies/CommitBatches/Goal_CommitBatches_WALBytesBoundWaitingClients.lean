import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! WAL group-commit claim for WALBytesBoundWaitingClients. The proof fixes what a contiguous durable
cut may acknowledge while keeping the same client-visible commit order. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_WALBytesBoundWaitingClients

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

theorem refute : ¬ WALBytesBoundWaitingClients.Goal := by
  intro h
  have hc := h [⟨0,1⟩, ⟨1,1⟩] 0 1 (by decide)
  exact (by native_decide : ¬ (([⟨0,1⟩, ⟨1,1⟩] : List Waiter).length ≤ 1 - 0)) hc

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CommitBatches_WALBytesBoundWaitingClients
