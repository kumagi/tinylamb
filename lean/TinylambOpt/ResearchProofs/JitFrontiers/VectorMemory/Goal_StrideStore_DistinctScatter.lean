import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideStore_DistinctScatter

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.StrideStore
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : StrideStore.DistinctScatter.Goal := by
  exact stride_distinct

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideStore_DistinctScatter
