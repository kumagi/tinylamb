import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `StrideRead.BroadcastRead` — `∀ j ids, j.stride=0 → ids≠[] → addresses kernel j
  ids=[j.src]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_BroadcastRead

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.StrideRead
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : StrideRead.BroadcastRead.Goal := by
  exact stride_broadcast

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_BroadcastRead
