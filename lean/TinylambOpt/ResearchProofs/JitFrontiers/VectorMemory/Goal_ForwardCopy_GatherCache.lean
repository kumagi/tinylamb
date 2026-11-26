import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ForwardCopy_GatherCache

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.ForwardCopy
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : ForwardCopy.GatherCache.Goal := by
  exact gatherCache_generic ForwardCopy.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ForwardCopy_GatherCache
