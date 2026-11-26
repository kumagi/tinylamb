import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

/-!
A spillable operator's resource accounting must protect partitions still referenced by
execution.

Proved

- `DropReleaseOrder` — Multiple cleanup paths that drop and unpin the same block
  commute.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_DropReleaseOrder

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem sound : DropReleaseOrder.Goal := by
  intro s l
  cases s
  cases l
  simp [dropBlock, release]
  split <;> simp_all

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_DropReleaseOrder
