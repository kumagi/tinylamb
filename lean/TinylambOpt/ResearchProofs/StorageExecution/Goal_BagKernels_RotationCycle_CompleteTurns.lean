import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `RotationCycle.CompleteTurns` — `∀ n m s k, bag.mass ((n+1)*(m+1),m,s) k = if k≤m
  then n+1 else 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_CompleteTurns

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

theorem sound_CompleteTurns : RotationCycle.CompleteTurns.Goal := by
  intro n m s k
  by_cases h : k ≤ m
  · simp [RotationCycle.bag, h, Nat.mul_mod_left]
  · simp [RotationCycle.bag, h]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_CompleteTurns
