import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
Decimal parsing of a concatenated chunk equals independent parsing of each
chunk followed by one multiply-add merge: `magnitude (a ++ b)` equals
`magnitude a * 10^b.length + magnitude b`. The JIT may therefore parse fixed
ASCII blocks in parallel (one accumulator per block) and combine them with a
single constant scale, preserving the decoded decimal value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_DecimalBlockMultiplyAccumulate
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

theorem sound : DecimalBlockMultiplyAccumulate.Goal := by
  intro a b
  unfold magnitude
  rw [List.foldl_append, mag_fold]
  rfl

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_DecimalBlockMultiplyAccumulate
