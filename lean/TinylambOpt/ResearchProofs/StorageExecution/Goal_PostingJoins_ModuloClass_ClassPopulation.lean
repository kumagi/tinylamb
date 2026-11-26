import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_ClassPopulation

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass

theorem sound_ClassPopulation : ClassPopulation.Goal := by
  intro u t x
  simp [search]
  apply Nat.div_le_div_right
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_ClassPopulation
