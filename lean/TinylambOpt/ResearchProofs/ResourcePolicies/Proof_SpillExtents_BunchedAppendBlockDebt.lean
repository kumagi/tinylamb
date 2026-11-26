import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_BunchedAppendBlockDebt

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch proof: batching two appends into one reservation changes only how
    many allocation calls are made. The telescoped block charge is unchanged,
    so SQL spill writes may fuse reservations without altering the quota. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.BunchedAppendBlockDebt.Goal := by
  intro u n a b hu
  have h1 : blocks u n ≤ blocks u (n + a) := by
    unfold blocks
    exact Nat.div_le_div_right (by omega)
  have h2 : blocks u (n + a) ≤ blocks u (n + a + b) := by
    unfold blocks
    exact Nat.div_le_div_right (by omega)
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_BunchedAppendBlockDebt
