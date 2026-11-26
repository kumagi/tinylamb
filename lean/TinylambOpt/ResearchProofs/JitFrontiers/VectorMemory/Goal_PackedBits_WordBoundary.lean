import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_WordBoundary

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.PackedBits
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : PackedBits.WordBoundary.Goal := by
  exact wordBoundary

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_WordBoundary
