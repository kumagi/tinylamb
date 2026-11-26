import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `MinimumDelta.StrictSavings` — Sum net page gains after counting the fixed-width
  alternative and each restart entry.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_StrictSavings

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.MinimumDelta

private theorem min_add_sub_eq (a b : Nat) : min a b + (b - a) = b := by
  omega

private theorem chosen_add_saved (f : Writer) (xs : List Nat) :
    chosen f xs + saved f xs = (raw xs).length := by
  unfold chosen saved
  simpa using min_add_sub_eq (frame f xs).length (raw xs).length

theorem sound_StrictSavings : MinimumDelta.StrictSavings.Goal := by
  intro ps fee
  unfold charge
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hmul : fee * (ps.length + 1) = fee * ps.length + fee := by
        rw [Nat.mul_add, Nat.mul_one]
      have hp := chosen_add_saved MinimumDelta.write p
      omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_StrictSavings
