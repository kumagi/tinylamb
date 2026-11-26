import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_BackwardDelta_ChunkLocal

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.BackwardDelta
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : BackwardDelta.ChunkLocal.Goal := by
  exact chunkLocal_generic BackwardDelta.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_BackwardDelta_ChunkLocal
