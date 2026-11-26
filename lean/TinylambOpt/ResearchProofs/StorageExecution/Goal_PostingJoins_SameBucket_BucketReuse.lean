import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_BucketReuse

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket

theorem sound_BucketReuse : SameBucket.BucketReuse.Goal := by
  intro u t x z h
  simp [search, h]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_BucketReuse
