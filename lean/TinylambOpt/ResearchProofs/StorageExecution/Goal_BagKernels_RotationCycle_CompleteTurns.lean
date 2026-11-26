import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

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
