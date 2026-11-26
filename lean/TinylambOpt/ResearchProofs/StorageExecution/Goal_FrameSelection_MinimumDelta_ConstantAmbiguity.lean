import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_ConstantAmbiguity

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.MinimumDelta
open TinylambOpt.ResearchGoals.Compression.Wire

-- Truth check on a small grid: the all-zero frame always costs exactly two bytes.
#eval (List.range 8).map (fun n => (n, (write (List.replicate n 0)).length,
  (write (List.replicate n 1)).length, (write (List.replicate n 200)).length))

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem uleb_zero : uleb 0 = [0] := by
  unfold uleb
  simp

private theorem foldl_min_replicate_zero : ∀ n, List.foldl min 0 (List.replicate n 0) = 0
  | 0 => rfl
  | n+1 => by simp [List.replicate_succ, foldl_min_replicate_zero]

private theorem foldl_max_replicate_zero : ∀ n, List.foldl max 0 (List.replicate n 0) = 0
  | 0 => rfl
  | n+1 => by simp [List.replicate_succ, foldl_max_replicate_zero]

private theorem delta_replicate_zero : ∀ n, delta 0 (List.replicate n 0) = List.replicate n 0 := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih => simp [List.replicate_succ, delta, ih, zig]

private theorem min_replicate_zero : ∀ n, ((List.replicate n 0).min?.getD 0) = 0 := by
  intro n
  cases n with
  | zero => rfl
  | succ n => simp [List.min?, List.replicate_succ, foldl_min_replicate_zero]

private theorem peak_replicate_zero : ∀ n, peak (List.replicate n 0) = 0 := by
  intro n
  cases n with
  | zero => rfl
  | succ n => simp [peak, List.max?, List.replicate_succ, foldl_max_replicate_zero]

private theorem write_replicate_zero_len (n : Nat) : (write (List.replicate n 0)).length = 2 := by
  unfold write
  rw [delta_replicate_zero n]
  dsimp
  rw [min_replicate_zero n]
  simp only [Nat.sub_zero, List.map_replicate]
  rw [peak_replicate_zero n]
  simp [width, byteCount, pack, little, uleb_zero]

-- Every frame starts with two ULEB headers, each at least one byte long.
private theorem write_len_ge_two (xs : List Nat) : 2 ≤ (write xs).length := by
  unfold write
  dsimp
  simp only [List.length_append]
  have h1 := uleb_len_pos ((delta 0 xs).min?.getD 0)
  have h2 := uleb_len_pos (width (peak ((delta 0 xs).map (fun v => v - ((delta 0 xs).min?.getD 0)))))
  omega

theorem sound_ConstantAmbiguity : MinimumDelta.ConstantAmbiguity.Goal := by
  intro x n hn
  rw [write_replicate_zero_len n]
  exact write_len_ge_two (List.replicate n x)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_ConstantAmbiguity
