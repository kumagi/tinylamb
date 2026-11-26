import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_ConsecutiveKeys

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.OrdinalResidual

private theorem zipIdx_append {α} (xs ys : List α) (n : Nat) :
    (xs ++ ys).zipIdx n = xs.zipIdx n ++ ys.zipIdx (n + xs.length) := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      simpa [Nat.add_assoc, Nat.add_left_comm, Nat.add_comm] using ih (n + 1)

private theorem replicate_succ_zero (n : Nat) :
    List.replicate n 0 ++ [0] = List.replicate (n + 1) 0 := by
  induction n with
  | zero => rfl
  | succ n ih => simp [List.replicate_succ, ih]

private theorem residuals_append (xs ys : List Nat) :
    residuals (xs ++ ys) =
      residuals xs ++ (ys.zipIdx xs.length).map (fun (v, i) => zig ((v : Int) - i)) := by
  simp [residuals, zipIdx_append]

private theorem residuals_range : ∀ n, residuals (List.range n) = List.replicate n 0 := by
  intro n
  induction n with
  | zero => simp [residuals]
  | succ n ih =>
      rw [List.range_succ]
      rw [residuals_append]
      simp only [List.length_range]
      rw [ih]
      simp [zig, replicate_succ_zero]

private theorem peak_replicate_zero : ∀ n, peak (List.replicate n 0) = 0 := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih => simp [peak]

private theorem bitWidth_range : ∀ n, bitWidth (List.range n) = 0 := by
  intro n
  simp [bitWidth, residuals_range, peak_replicate_zero, width]

private theorem write_range_length : ∀ n, (write (List.range n)).length = 1 := by
  intro n
  simp [write, bitWidth_range, pack, little, uleb, byteCount]

theorem sound_ConsecutiveKeys : OrdinalResidual.ConsecutiveKeys.Goal := by
  exact write_range_length

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_ConsecutiveKeys
