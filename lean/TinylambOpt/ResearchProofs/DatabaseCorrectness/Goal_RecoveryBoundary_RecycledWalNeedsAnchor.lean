import TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

/-!
A zero replay anchor with no loaded checkpoint denotes the beginning of the
recycled WAL, which cannot be used as a replay start for a nonempty database.
Recovery must refuse to start rather than treat those bytes as an empty WAL.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_RecycledWalNeedsAnchor

open TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

theorem sound : RecycledWalNeedsAnchor.Goal := by
  intro first hfirst
  simp [startAt, anchor, hfirst]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_RecycledWalNeedsAnchor
