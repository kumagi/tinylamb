import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `Cooldown.SpacingRecurrence` — `∀ n, count model (n+3) 0 = count model (n+2) 0 +
  count model n 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_SpacingRecurrence

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.Cooldown

private theorem count_succ_zero (n : Nat) :
    count model (n + 1) 0 = count model n 0 + count model n 2 := by
  simp [count, model]

private theorem count_succ_one (n : Nat) :
    count model (n + 1) 1 = count model n 0 := by
  simp [count, model]

private theorem count_succ_two (n : Nat) :
    count model (n + 1) 2 = count model n 1 := by
  simp [count, model]

theorem sound_SpacingRecurrence : Cooldown.SpacingRecurrence.Goal := by
  intro n
  calc
    count model (n + 3) 0 = count model (n + 2) 0 + count model (n + 2) 2 := by
      simpa [Nat.add_assoc] using count_succ_zero (n + 2)
    _ = count model (n + 2) 0 + count model (n + 1) 1 := by
      rw [count_succ_two (n + 1)]
    _ = count model (n + 2) 0 + count model n 0 := by
      rw [count_succ_one n]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_SpacingRecurrence
