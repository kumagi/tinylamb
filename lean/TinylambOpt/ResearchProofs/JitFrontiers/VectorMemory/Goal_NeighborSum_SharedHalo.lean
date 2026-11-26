import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `NeighborSum.SharedHalo` — `∀ j n, 0<n → (addresses kernel j (List.range
  n)).length=n+1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_NeighborSum_SharedHalo

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.NeighborSum
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : NeighborSum.SharedHalo.Goal := by
  exact sharedHalo

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_NeighborSum_SharedHalo
