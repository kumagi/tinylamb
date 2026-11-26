import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `IndexedGather.GatherCache` — Common loads may be cached once even for repeated
  ordinals.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedGather_GatherCache

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.IndexedGather
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : IndexedGather.GatherCache.Goal := by
  exact gatherCache_generic IndexedGather.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedGather_GatherCache
