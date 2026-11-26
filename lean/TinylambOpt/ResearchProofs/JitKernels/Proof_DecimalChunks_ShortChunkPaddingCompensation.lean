import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
Padding a digit string on the right with `n` ASCII zeros scales its magnitude
by exactly `10^n`, so an integer division by `10^n` recovers the original
value: `magnitude (s ++ replicate n 48) / 10^n = magnitude s`. Short final
chunks may be zero-padded to a full SIMD lane width and rescaled afterwards.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_ShortChunkPaddingCompensation
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

private theorem mag_fold (s : List Nat) (m : Nat) :
    List.foldl (fun n c => 10*n+(c-48)) m s = m * 10^s.length + magnitude s := by
  induction s generalizing m with
  | nil =>
      rw [show List.foldl (fun n c => 10*n+(c-48)) m [] = m from rfl,
        show magnitude [] = 0 from rfl, show List.length ([] : List Nat) = 0 from rfl,
        Nat.pow_zero, Nat.mul_one, Nat.add_zero]
  | cons c t ih =>
      rw [List.foldl_cons, ih]
      have hc : magnitude (c :: t) = List.foldl (fun n x => 10*n+(x-48)) (c-48) t := by
        have h2 : magnitude (c :: t) = List.foldl (fun n x => 10*n+(x-48)) (10*0+(c-48)) t := rfl
        rw [h2, show (10*0+(c-48)) = c-48 from by simp]
      rw [hc, ih (c-48), List.length_cons, Nat.pow_succ]
      simp [Nat.add_mul, Nat.mul_add, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm, Nat.add_assoc]

private theorem mag_block (a b : List Nat) :
    magnitude (a ++ b) = magnitude a * 10^b.length + magnitude b := by
  unfold magnitude
  rw [List.foldl_append, mag_fold]
  rfl

private theorem mag_rep48 (n : Nat) : magnitude (List.replicate n 48) = 0 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.replicate_succ]
      have h2 : magnitude (48 :: List.replicate n 48) =
          List.foldl (fun n c => 10*n+(c-48)) (10*0+(48-48)) (List.replicate n 48) := rfl
      rw [h2, show (10*0+(48-48)) = 0 from by decide]
      exact ih

theorem sound : ShortChunkPaddingCompensation.Goal := by
  intro s n
  have hp : 0 < 10^n := by
    have h1 : 1 ≤ 10^n := Nat.one_le_pow _ _ (by decide)
    omega
  rw [mag_block, mag_rep48, Nat.add_zero, List.length_replicate]
  exact Nat.mul_div_cancel _ hp

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_ShortChunkPaddingCompensation
