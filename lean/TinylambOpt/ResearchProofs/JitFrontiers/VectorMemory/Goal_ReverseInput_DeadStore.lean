import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_DeadStore

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.ReverseInput
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : ReverseInput.DeadStore.Goal := by
  exact deadStore_generic ReverseInput.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_DeadStore
