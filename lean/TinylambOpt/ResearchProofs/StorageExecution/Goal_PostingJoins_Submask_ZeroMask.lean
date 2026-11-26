import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Submask.ZeroMask` — `∀ u t, 0<u → search.spans u t 0=[(0,1)]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_ZeroMask

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask

theorem sound_ZeroMask : Submask.ZeroMask.Goal := by
  intro u t hu
  simp [Submask.search, submasks, TinylambOpt.ResearchGoals.Compression.Wire.width, hu]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_ZeroMask
