import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ModuloClass.ResidueCache` — `∀ u t x k ys, seek search u t (x+k*(t+1)) ys = seek
  search u t x ys`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_ResidueCache

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass

theorem sound_ResidueCache : ResidueCache.Goal := by
  intro u t x k ys
  unfold seek
  congr 1
  dsimp [search]
  have hmod : (x + k * (t + 1)) % (t + 1) = x % (t + 1) := by
    simp [Nat.add_mod]
  rw [hmod]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_ResidueCache
