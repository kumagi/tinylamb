import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `EdgeDegrees.LoopDegree` — `∀ es x, bag.mass ((x,x)::es) x = bag.mass es x+2`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_LoopDegree

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.EdgeDegrees

-- Sanity checks on small inputs (empty tail, repeated keys, and a fresh loop key).
#eval bag.mass ([(3, 3)]) 3
#eval bag.mass ([]) 3 + 2
example : bag.mass ([(3, 3)]) 3 = bag.mass ([]) 3 + 2 := by decide
example : bag.mass ([(3, 3), (1, 3), (3, 7)]) 3 = bag.mass ([(1, 3), (3, 7)]) 3 + 2 := by decide
example : bag.mass ([(9, 9), (0, 1)]) 9 = bag.mass ([(0, 1)]) 9 + 2 := by decide

theorem sound_LoopDegree : EdgeDegrees.LoopDegree.Goal := by
  intro es x
  unfold bag
  simp
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_LoopDegree
