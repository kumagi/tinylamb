import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `Cooldown.CheckpointConvergence` — `∀ q, q≤2 → run model q [false,false]=some 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_CheckpointConvergence

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.Cooldown

private def smallCheck : Bool :=
  (List.range 3).all fun q =>
    run model q [false, false] == some 0

example : smallCheck = true := by native_decide

theorem sound_CheckpointConvergence : CheckpointConvergence.Goal := by
  intro q hq
  have hcases : q = 0 ∨ q = 1 ∨ q = 2 := by omega
  rcases hcases with rfl | rfl | rfl <;> native_decide

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_CheckpointConvergence
