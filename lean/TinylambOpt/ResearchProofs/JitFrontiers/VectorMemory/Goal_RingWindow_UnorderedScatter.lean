import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_UnorderedScatter

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.RingWindow
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : RingWindow.UnorderedScatter.Goal := by
  exact unorderedScatter_generic RingWindow.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_UnorderedScatter
