import TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_Publication_DelayedReleaseOrderIndependence

open TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication


theorem sound : DelayedReleaseOrderIndependence.Goal := by
  intro c x y
  simp [announce, Nat.max_assoc, Nat.max_comm, Nat.max_left_comm]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_Publication_DelayedReleaseOrderIndependence
