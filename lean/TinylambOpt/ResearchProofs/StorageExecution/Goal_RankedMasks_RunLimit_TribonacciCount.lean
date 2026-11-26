import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `RunLimit.TribonacciCount` — `∀ n, count model (n+3) 0 = count model (n+2) 0 + count
  model (n+1) 0 + count model n 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_TribonacciCount

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.RunLimit

private theorem count_succ_zero (n : Nat) :
    count model (n + 1) 0 = count model n 0 + count model n 1 := by
  simp [count, model]

private theorem count_succ_one (n : Nat) :
    count model (n + 1) 1 = count model n 0 + count model n 2 := by
  simp [count, model]

private theorem count_succ_two (n : Nat) :
    count model (n + 1) 2 = count model n 0 := by
  simp [count, model]

theorem sound_TribonacciCount : RunLimit.TribonacciCount.Goal := by
  intro n
  calc
    count model (n + 3) 0 = count model (n + 2) 0 + count model (n + 2) 1 := by
      simpa [Nat.add_assoc] using count_succ_zero (n + 2)
    _ = count model (n + 2) 0 + (count model (n + 1) 0 + count model (n + 1) 2) := by
      rw [count_succ_one (n + 1)]
    _ = count model (n + 2) 0 + (count model (n + 1) 0 + count model n 0) := by
      rw [count_succ_two n]
    _ = count model (n + 2) 0 + count model (n + 1) 0 + count model n 0 := by
      omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_TribonacciCount
