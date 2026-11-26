import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `NeighborSum.InPlaceForward` — `∀ j n, j.src=j.dst → rawFree kernel j (List.range n)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_NeighborSum_InPlaceForward

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.NeighborSum
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : NeighborSum.InPlaceForward.Goal := by
  exact neighbor_inplace

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_NeighborSum_InPlaceForward
