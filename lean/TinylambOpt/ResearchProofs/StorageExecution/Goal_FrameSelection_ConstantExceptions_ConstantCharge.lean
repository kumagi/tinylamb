import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `ConstantExceptions.ConstantCharge` — `∀ x n, (write (List.replicate n x)).length =
  5`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_ConstantCharge

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.ConstantExceptions
open TinylambOpt.ResearchGoals.Compression.Wire

#eval (write (List.replicate 0 7)).length
#eval (write (List.replicate 1 7)).length
#eval (write (List.replicate 5 7)).length
#eval (write (List.replicate 300 0)).length

private theorem little_four_length (x : Nat) : (little x 4).length = 4 := by
  simp [little]

private theorem uleb_zero_length : (uleb 0).length = 1 := by
  native_decide

private theorem zipIdx_nil {α : Type} (i : Nat) : ([] : List α).zipIdx i = [] := by
  rfl

private theorem filter_replicate_zipIdx (x n start : Nat) :
    ((List.replicate n x).zipIdx start).filter (fun p => match p with | (v, _) => v != x) = [] := by
  induction n generalizing start with
  | zero => simp
  | succ n ih =>
      simp [List.replicate_succ, ih]

theorem sound_ConstantCharge : ConstantExceptions.ConstantCharge.Goal := by
  intro x n
  unfold write
  cases n with
  | zero =>
      rw [List.replicate_zero, List.head?_nil, Option.getD_none]
      dsimp only
      rw [zipIdx_nil, List.filter_nil, List.flatMap_nil]
      simp only [List.length_nil]
      rw [List.length_append, List.length_append, little_four_length, uleb_zero_length]
      simp only [List.length_nil]
  | succ n =>
      rw [List.replicate_succ, List.head?_cons, Option.getD_some]
      rw [List.zipIdx_cons]
      dsimp only
      rw [List.filter_cons_of_neg]
      · rw [filter_replicate_zipIdx]
        change (little x 4 ++ uleb 0 ++ []).length = 5
        rw [List.length_append, List.length_append, little_four_length, uleb_zero_length]
        simp only [List.length_nil]
      · simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_ConstantCharge
