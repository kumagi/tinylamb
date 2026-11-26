import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_NegativeTest

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

-- Small-input truth check: every sampled list has its residue bit set for
-- each of its members, so the claimed implication holds on all samples.
private def mask (xs : List Nat) : Nat :=
  xs.foldl (fun a x => a ||| 2 ^ (x % 64)) 0

private def samples : List (List Nat) :=
  [[], [0], [1], [64], [63, 64, 65], [3, 67, 3], [0, 1, 2, 63, 64, 65]]

private def probe : Bool :=
  samples.all (fun xs => (List.range 130).all (fun x =>
    (mask xs).testBit (x % 64) || ! xs.contains x))

#eval probe

private theorem testBit_fold (xs : List Nat) (a j : Nat) :
    (xs.foldl (fun a x => a ||| 2 ^ (x % 64)) a).testBit j =
      (a.testBit j || xs.any (fun x => x % 64 == j)) := by
  induction xs generalizing a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih (a ||| 2 ^ (x % 64))]
      simp only [Nat.testBit_or, Nat.testBit_two_pow, List.any_cons]
      by_cases hEq : x % 64 = j
      · simp [hEq]
      · have hbeq : (x % 64 == j) = false := beq_eq_false_iff_ne.mpr hEq
        simp [hEq, hbeq]

private theorem mask_testBit (xs : List Nat) (j : Nat) :
    (mask xs).testBit j = xs.any (fun x => x % 64 == j) := by
  simpa [mask] using testBit_fold xs 0 j

theorem sound_NegativeTest : BitOccupancy.NegativeTest.Goal := by
  intro xs x h hx
  simp [agg] at h
  change (mask xs).testBit (x % 64) = false at h
  have hbit : (mask xs).testBit (x % 64) = true := by
    rw [mask_testBit xs (x % 64)]
    exact List.any_eq_true.mpr ⟨x, hx, by simp⟩
  simp [hbit] at h

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_NegativeTest
