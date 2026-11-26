import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_FoldInto32

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 31 :: xs, 32 :: xs, 63 :: xs, 64 :: xs, 65 :: xs, 96 :: xs])

private def probe (n : Nat) : Bool :=
  (gen n).all (fun xs => (List.range 32).all (fun k =>
    ((agg.reference xs).2.testBit k || (agg.reference xs).2.testBit (k + 32)) == xs.any (fun x => x % 32 == k)))

-- Small-input truth check: both enumerations return `true`.
#eval probe 1
#eval probe 2

private def mask (xs : List Nat) : Nat :=
  xs.foldl (fun a x => a ||| 2 ^ (x % 64)) 0

private theorem testBit_fold (xs : List Nat) (a j : Nat) :
    (xs.foldl (fun a x => a ||| 2 ^ (x % 64)) a).testBit j =
      (a.testBit j || xs.any (fun x => x % 64 == j)) := by
  induction xs generalizing a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons, List.any_cons]
      rw [ih (a ||| 2 ^ (x % 64))]
      simp only [Nat.testBit_or, Nat.testBit_two_pow]
      by_cases h : x % 64 = j
      · simp [h]
      · have hb : (x % 64 == j) = false := by simp [h]
        rw [hb]
        simp [h]

private theorem mask_testBit (xs : List Nat) (j : Nat) :
    (mask xs).testBit j = xs.any (fun x => x % 64 == j) := by
  simpa [mask] using testBit_fold xs 0 j

private theorem mod64_eq_or (x k : Nat) (hk : k < 32) :
    x % 32 = k ↔ x % 64 = k ∨ x % 64 = k + 32 := by
  have hlt : x % 64 < 64 := Nat.mod_lt _ (by decide : 0 < 64)
  have hmod : x % 64 % 32 = x % 32 := Nat.mod_mod_of_dvd x (by decide : 32 ∣ 64)
  constructor
  · intro h
    have hk' : x % 64 % 32 = k := by omega
    omega
  · intro h
    rcases h with h | h <;> omega

private theorem mod64_bool (x k : Nat) (hk : k < 32) :
    (x % 64 == k || x % 64 == k + 32) = (x % 32 == k) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    simp only [Bool.or_eq_true] at h
    rcases h with h | h
    · exact beq_iff_eq.mpr ((mod64_eq_or x k hk).mpr (Or.inl (beq_iff_eq.mp h)))
    · exact beq_iff_eq.mpr ((mod64_eq_or x k hk).mpr (Or.inr (beq_iff_eq.mp h)))
  · intro h
    have hcases := (mod64_eq_or x k hk).mp (beq_iff_eq.mp h)
    simp only [Bool.or_eq_true]
    rcases hcases with hcase | hcase
    · exact Or.inl (beq_iff_eq.mpr hcase)
    · exact Or.inr (beq_iff_eq.mpr hcase)

private theorem any_fold (xs : List Nat) (k : Nat) (hk : k < 32) :
    (xs.any (fun x => x % 64 == k) || xs.any (fun x => x % 64 == k + 32)) =
      xs.any (fun x => x % 32 == k) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.any_cons]
      have hpt := mod64_bool x k hk
      cases ha : x % 64 == k <;>
        cases hb : x % 64 == k + 32 <;>
        cases hc : x % 32 == k <;>
        cases hp : xs.any (fun y => y % 64 == k) <;>
        cases hq : xs.any (fun y => y % 64 == k + 32) <;>
        cases hr : xs.any (fun y => y % 32 == k) <;>
        simp [ha, hb, hc, hp, hq, hr] at ih hpt ⊢

theorem sound_FoldInto32 : BitOccupancy.FoldInto32.Goal := by
  intro xs k hk
  change ((mask xs).testBit k || (mask xs).testBit (k + 32)) =
    xs.any (fun x => x % 32 == k)
  rw [mask_testBit xs k, mask_testBit xs (k + 32)]
  exact any_fold xs k hk

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_FoldInto32
