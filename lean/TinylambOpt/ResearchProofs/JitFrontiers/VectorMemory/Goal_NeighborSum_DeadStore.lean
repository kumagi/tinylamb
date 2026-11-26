import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_NeighborSum_DeadStore

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.NeighborSum
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : NeighborSum.DeadStore.Goal := by
  exact deadStore_generic NeighborSum.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_NeighborSum_DeadStore
