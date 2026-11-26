import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CommitBatches_WALBytesBoundWaitingClients
open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches
open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches.WALBytesBoundWaitingClients
theorem refute : ¬ Goal := by
  intro h
  let w1 : Waiter := ⟨1, 1⟩
  let w2 : Waiter := ⟨2, 1⟩
  have := h [w1, w2] 0 1 (by
    intro w hw
    simp only [List.mem_cons, List.mem_nil_iff, or_false] at hw
    rcases hw with rfl | rfl <;> decide)
  simp [w1, w2] at this
end Nov26.Proof_CommitBatches_WALBytesBoundWaitingClients
