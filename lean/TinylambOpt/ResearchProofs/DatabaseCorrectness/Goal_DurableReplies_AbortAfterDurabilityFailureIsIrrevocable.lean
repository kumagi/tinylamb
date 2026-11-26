import TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

/-! Aborting after a failed post-publication durability wait leaves the
    irrevocably published phase and the failure reply intact. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_AbortAfterDurabilityFailureIsIrrevocable

open TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

theorem sound : AbortAfterDurabilityFailureIsIrrevocable.Goal := by
  intro t d
  simp [launch, complete, abort]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_AbortAfterDurabilityFailureIsIrrevocable
