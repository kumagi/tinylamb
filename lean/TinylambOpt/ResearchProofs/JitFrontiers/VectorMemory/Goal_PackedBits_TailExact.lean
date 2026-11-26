import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_TailExact

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.PackedBits
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : PackedBits.TailExact.Goal := by
  exact tailExact_generic PackedBits.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_TailExact
