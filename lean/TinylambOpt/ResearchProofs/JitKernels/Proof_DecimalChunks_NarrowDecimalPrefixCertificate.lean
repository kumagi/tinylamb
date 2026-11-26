import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
Appending decimal digits never decreases the accumulated magnitude, so any
prefix of a digit string has magnitude at most the whole string's magnitude.
If the full value fits under `2^w`, no intermediate multiply-add while
scanning the string can have overflowed a `w`-bit accumulator:
`magnitude s < 2^w → magnitude (s.take n) < 2^w`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_NarrowDecimalPrefixCertificate
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

private theorem mag_extend (a b : List Nat) : magnitude a ≤ magnitude (a ++ b) := by
  rw [mag_block]
  have hp : 0 < 10^b.length := by
    have h1 : 1 ≤ 10^b.length := Nat.one_le_pow _ _ (by decide)
    omega
  have h1 : magnitude a ≤ magnitude a * 10^b.length := Nat.le_mul_of_pos_right _ hp
  exact Nat.le_trans h1 (Nat.le_add_right _ _)

private theorem mag_take (s : List Nat) (n : Nat) : magnitude (s.take n) ≤ magnitude s := by
  have h := mag_extend (s.take n) (s.drop n)
  rw [List.take_append_drop] at h
  exact h

theorem sound : NarrowDecimalPrefixCertificate.Goal := by
  intro s w n hd hm
  exact Nat.lt_of_le_of_lt (mag_take s n) hm

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_NarrowDecimalPrefixCertificate
