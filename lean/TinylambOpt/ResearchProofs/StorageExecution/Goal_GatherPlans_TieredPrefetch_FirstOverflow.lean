import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_FirstOverflow

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.TieredPrefetch

-- Truth check on small inputs: the first extent past the fast-buffer limit
-- always has span `lim + 1`, hence spills exactly once.
#eval ((List.range 8).all (fun lim =>
        (List.range 8).all (fun fee =>
          (List.range 8).all (fun x =>
            price lim fee x (x + lim) = lim + fee + 3))))

theorem sound_FirstOverflow : TieredPrefetch.FirstOverflow.Goal := by
  intro lim fee x
  unfold TieredPrefetch.price span
  have hspan : x + lim + 1 - x = lim + 1 := by omega
  rw [hspan]
  have hlt : lim < lim + 1 := by omega
  simp [hlt]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_FirstOverflow
