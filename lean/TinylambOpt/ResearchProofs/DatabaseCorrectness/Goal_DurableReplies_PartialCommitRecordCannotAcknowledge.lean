import TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

/-! A commit record whose tail is still beyond the durable prefix blocks the
    synchronous reply even when its first byte is durable. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_PartialCommitRecordCannotAcknowledge

open TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

theorem sound : PartialCommitRecordCannotAcknowledge.Goal := by
  intro start size observed d hsize hsd hds
  have hnle : ¬ target ⟨start + size, true, observed⟩ ≤ d := by
    dsimp [target]
    omega
  simp [launch, complete, hnle]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_PartialCommitRecordCannotAcknowledge
