import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ScaledFloor.BucketWidth` — `∀ t x, search.spans ((t+1)*(x+1)) t
  x=[(t*(x+1),(t+1)*(x+1))]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_BucketWidth

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor

-- truth check on small inputs
#eval (List.range 8).all (fun t =>
  (List.range 8).all (fun x =>
    search.spans ((t+1)*(x+1)) t x == [(t*(x+1),(t+1)*(x+1))]))

theorem sound_BucketWidth : ScaledFloor.BucketWidth.Goal := by
  intro t x
  simp [ScaledFloor.search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_BucketWidth
