import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `StrideRead.GatherCache` — Common loads may be cached once even for repeated
  ordinals.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_GatherCache

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.StrideRead
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : StrideRead.GatherCache.Goal := by
  exact gatherCache_generic StrideRead.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_GatherCache
