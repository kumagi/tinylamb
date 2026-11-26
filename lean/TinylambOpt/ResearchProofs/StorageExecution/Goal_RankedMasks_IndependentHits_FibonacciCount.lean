import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_FibonacciCount

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits

private theorem count_succ_zero (n : Nat) :
    count model (n + 1) 0 = count model n 0 + count model n 1 := by
  simp [count, model]

private theorem count_succ_one (n : Nat) :
    count model (n + 1) 1 = count model n 0 := by
  simp [count, model]

theorem sound_FibonacciCount : IndependentHits.FibonacciCount.Goal := by
  intro n
  calc
    count model (n + 2) 0 = count model (n + 1) 0 + count model (n + 1) 1 := by
      simpa [Nat.add_assoc] using count_succ_zero (n + 1)
    _ = count model (n + 1) 0 + count model n 0 := by
      rw [count_succ_one n]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_FibonacciCount
