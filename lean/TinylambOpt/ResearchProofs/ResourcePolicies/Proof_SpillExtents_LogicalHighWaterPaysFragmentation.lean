import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_LogicalHighWaterPaysFragmentation

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch refutation: the logical high-water mark does not pay for block
    alignment. Many tiny segments each consume a whole block, so the physical
    charge can exceed the single rounded high-water allocation. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.LogicalHighWaterPaysFragmentation.Goal := by
  intro h
  have hbad := h 2 [1, 1] 2 (by decide) (by decide)
  exact (by decide : ¬ tailCharge 2 [1, 1] ≤ 2 * blocks 2 2) hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_LogicalHighWaterPaysFragmentation
