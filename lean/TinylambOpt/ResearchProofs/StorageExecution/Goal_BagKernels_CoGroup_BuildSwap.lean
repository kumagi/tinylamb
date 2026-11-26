import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `CoGroup.BuildSwap` — `∀ a b k, bag.mass (a,b) k = bag.mass (b,a) k`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_BuildSwap

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

-- Sanity check: both sides agree on a small instance.
#eval (bag.mass ([1, 2, 2, 3], [2, 3, 3, 4]) 2,
      bag.mass ([2, 3, 3, 4], [1, 2, 2, 3]) 2)

theorem sound_BuildSwap : BuildSwap.Goal := by
  intro a b k
  simp [bag, Nat.mul_comm]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_BuildSwap
