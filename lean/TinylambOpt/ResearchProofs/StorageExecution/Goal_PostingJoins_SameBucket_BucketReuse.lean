import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `SameBucket.BucketReuse` — `∀ u t x z, x/(t+1)=z/(t+1) → search.spans u t x =
  search.spans u t z`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_BucketReuse

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket

theorem sound_BucketReuse : SameBucket.BucketReuse.Goal := by
  intro u t x z h
  simp [search, h]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_BucketReuse
