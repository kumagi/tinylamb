import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_DependenceGuard

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.StrideRead
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : StrideRead.DependenceGuard.Goal := by
  exact dependenceGuard_generic StrideRead.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_DependenceGuard
