import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_PeriodicGather

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.RingWindow
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : RingWindow.PeriodicGather.Goal := by
  exact ring_periodic

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_PeriodicGather
