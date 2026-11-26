import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_PartialTailRollback

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch proof: undoing a short append by truncating back to the old logical
    length preserves the old partial tail byte-for-byte, so a canceled spill
    write may discard its unpublished payload without restoring extra state. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.PartialTailRollback.Goal := by
  intro old payload written
  simp
end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_PartialTailRollback
