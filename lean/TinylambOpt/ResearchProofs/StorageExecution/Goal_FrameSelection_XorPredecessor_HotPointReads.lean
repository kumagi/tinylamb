import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_HotPointReads

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.XorPredecessor

private def vals : List Nat := [0, 1, 127, 128, 256]

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => (gen n).flatMap (fun xs => vals.map (fun v => v :: xs))

-- Small-input truth check: no price inversion was found.
#eval ((gen 3).any (fun xs =>
  [0, 1, 10].any (fun fee =>
    (List.range 5).any (fun p =>
      (List.range 6).any (fun q =>
        decide (p < q) && decide (seekWork (plan write fee q xs) > seekWork (plan write fee p xs)))))))

private def objective (f : Writer) (fee price : Nat) (ps : List (List Nat)) : Nat :=
  charge f fee ps + price * seekWork ps

private theorem parts_ne_nil (xs : List Nat) : parts xs ≠ [] := by
  induction xs with
  | nil => simp [parts]
  | cons x xs ih =>
      intro h
      have hall := List.flatMap_eq_nil_iff.mp h
      cases hp : parts xs with
      | nil => exact ih hp
      | cons ps rest =>
          have hps := hall ps (by simp [hp])
          cases ps with
          | nil => simp at hps
          | cons a rest => simp at hps

private theorem mergeSort_head_min {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c : α, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b : α, (le a b || le b a) = true)
    {l : List α} {a : α} {t : List α} (h : l.mergeSort le = a :: t) :
    ∀ b ∈ l, le a b = true := by
  have hs := List.pairwise_mergeSort trans total l
  rw [h] at hs
  have hhead : ∀ b ∈ t, le a b = true := (List.pairwise_cons.mp hs).1
  intro b hb
  have hb' : b ∈ l.mergeSort le := by simpa using hb
  rw [h] at hb'
  simp only [List.mem_cons] at hb'
  rcases hb' with rfl | hbt
  · simpa using total b b
  · exact hhead b hbt

private theorem plan_mem_parts (f : Writer) (fee price : Nat) (xs : List Nat) :
    plan f fee price xs ∈ parts xs := by
  let le : List (List Nat) → List (List Nat) → Bool :=
    fun a b => decide (objective f fee price a ≤ objective f fee price b)
  change ((parts xs).mergeSort le).head?.getD [] ∈ parts xs
  cases hm : (parts xs).mergeSort le with
  | nil =>
      have hlen := List.length_mergeSort (parts xs) (le := le)
      rw [hm] at hlen
      exact (parts_ne_nil xs (List.length_eq_zero_iff.mp hlen.symm)).elim
  | cons a t =>
      change a ∈ parts xs
      have hmem : a ∈ (parts xs).mergeSort le := by
        rw [hm]
        exact List.mem_cons_self
      exact List.mem_mergeSort.mp hmem

private theorem plan_min (f : Writer) (fee price : Nat) (xs : List Nat) (ps : List (List Nat))
    (hps : ps ∈ parts xs) :
    objective f fee price (plan f fee price xs) ≤ objective f fee price ps := by
  let le : List (List Nat) → List (List Nat) → Bool :=
    fun a b => decide (objective f fee price a ≤ objective f fee price b)
  have htrans : ∀ a b c : List (List Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc))
  have htotal : ∀ a b : List (List Nat), (le a b || le b a) = true := by
    intro a b
    by_cases h : objective f fee price a ≤ objective f fee price b
    · simp [le, h]
    · have h' : objective f fee price b ≤ objective f fee price a := Nat.le_of_not_ge h
      simp [le, h, h']
  have hne : (parts xs).mergeSort le ≠ [] := by
    intro hm
    have hlen := List.length_mergeSort (parts xs) (le := le)
    rw [hm] at hlen
    exact parts_ne_nil xs (List.length_eq_zero_iff.mp hlen.symm)
  cases hm : (parts xs).mergeSort le with
  | nil => exact (hne hm).elim
  | cons a t =>
      have hle := mergeSort_head_min le htrans htotal hm ps hps
      have hnat : objective f fee price a ≤ objective f fee price ps :=
        of_decide_eq_true hle
      change objective f fee price (((parts xs).mergeSort le).head?.getD []) ≤
        objective f fee price ps
      rw [hm]
      exact hnat

theorem sound_HotPointReads : XorPredecessor.HotPointReads.Goal := by
  intro xs fee p q hpq
  let A := plan write fee p xs
  let B := plan write fee q xs
  have hA : objective write fee p A ≤ objective write fee p B :=
    plan_min write fee p xs B (plan_mem_parts write fee q xs)
  have hB : objective write fee q B ≤ objective write fee q A :=
    plan_min write fee q xs A (plan_mem_parts write fee p xs)
  change seekWork B ≤ seekWork A
  apply Nat.le_of_not_gt
  intro hlt
  have hpos : 0 < q - p := by omega
  have hmul : (q - p) * seekWork A < (q - p) * seekWork B :=
    Nat.mul_lt_mul_of_pos_left hlt hpos
  have hq : q = p + (q - p) := by omega
  have hsumA : q * seekWork A = p * seekWork A + (q - p) * seekWork A := by
    have h := congrArg (fun z => z * seekWork A) hq
    rw [Nat.add_mul] at h
    exact h
  have hsumB : q * seekWork B = p * seekWork B + (q - p) * seekWork B := by
    have h := congrArg (fun z => z * seekWork B) hq
    rw [Nat.add_mul] at h
    exact h
  simp only [objective] at hA hB
  have hkey : (q - p) * seekWork B ≤ (q - p) * seekWork A := by omega
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_HotPointReads
