import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_HotPointReads

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.DictionaryRuns

-- Truth check on small inputs before proving the general statement.
#eval ([([], 0, 0, 3), ([0], 1, 0, 2), ([0, 1], 0, 1, 4),
        ([1, 2, 3], 2, 0, 5), ([0, 1, 0, 1], 0, 2, 7)].all
  (fun (xs, fee, p, q) =>
    decide (p < q && seekWork (plan write fee q xs) ≤ seekWork (plan write fee p xs))))

private def cost (f : Writer) (fee price : Nat) (ps : List (List Nat)) : Nat :=
  charge f fee ps + price * seekWork ps

private theorem parts_nonempty : ∀ xs : List Nat, ∃ ps, ps ∈ parts xs := by
  intro xs
  induction xs with
  | nil =>
      refine ⟨[], ?_⟩
      simp [parts]
  | cons x xs ih =>
      rcases ih with ⟨q, hq⟩
      cases q with
      | nil =>
          refine ⟨[[x]], ?_⟩
          rw [parts]
          apply List.mem_flatMap.mpr
          exact ⟨[], hq, by simp⟩
      | cons a rest =>
          refine ⟨[x] :: a :: rest, ?_⟩
          rw [parts]
          apply List.mem_flatMap.mpr
          exact ⟨a :: rest, hq, by simp⟩

-- `mergeSort` with a total, transitive `≤` comparator puts a minimum first.
private theorem plan_mem (f : Writer) (fee price : Nat) (xs : List Nat) :
    plan f fee price xs ∈ parts xs := by
  let le : List (List Nat) → List (List Nat) → Bool := fun a b =>
    charge f fee a + price * seekWork a ≤ charge f fee b + price * seekWork b
  have hperm := List.mergeSort_perm (parts xs) le
  have hsne : (parts xs).mergeSort le ≠ [] := by
    intro hs
    rcases parts_nonempty xs with ⟨ps0, hps0⟩
    have : ps0 ∈ (parts xs).mergeSort le := (hperm.mem_iff).2 hps0
    simp [hs] at this
  rcases List.exists_cons_of_ne_nil hsne with ⟨hd, tl, hs⟩
  have hdm : hd ∈ parts xs := by
    have : hd ∈ (parts xs).mergeSort le := by simp [hs]
    exact (hperm.mem_iff).1 this
  have hplan : plan f fee price xs = hd := by
    unfold plan
    change (((parts xs).mergeSort le).head?.getD []) = hd
    simp [hs]
  simpa [hplan] using hdm

private theorem plan_is_min (f : Writer) (fee price : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
    cost f fee price (plan f fee price xs) ≤ cost f fee price ps := by
  let le : List (List Nat) → List (List Nat) → Bool := fun a b =>
    charge f fee a + price * seekWork a ≤ charge f fee b + price * seekWork b
  have htrans : ∀ a b c : List (List Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    dsimp [le] at hab hbc ⊢
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc))
  have htotal : ∀ a b : List (List Nat), (le a b || le b a) = true := by
    intro a b
    dsimp [le]
    cases Nat.le_total (charge f fee a + price * seekWork a) (charge f fee b + price * seekWork b) with
    | inl h => simp [h]
    | inr h => simp [h]
  have hperm := List.mergeSort_perm (parts xs) le
  have hsne : (parts xs).mergeSort le ≠ [] := by
    intro hs
    rcases parts_nonempty xs with ⟨ps0, hps0⟩
    have : ps0 ∈ (parts xs).mergeSort le := (hperm.mem_iff).2 hps0
    simp [hs] at this
  rcases List.exists_cons_of_ne_nil hsne with ⟨hd, tl, hs⟩
  have hpw := List.pairwise_mergeSort htrans htotal (parts xs)
  have hpw' : List.Pairwise (fun a b => le a b = true) (hd :: tl) := by simpa [hs] using hpw
  have hforall := (List.pairwise_cons.mp hpw').1
  have hlehd : le hd ps = true := by
    by_cases heq : ps = hd
    · subst ps
      dsimp [le]
      simp
    · have hps_s : ps ∈ (parts xs).mergeSort le := (hperm.mem_iff).2 hps
      have hps_tl : ps ∈ tl := by
        simpa [hs, heq] using hps_s
      exact hforall ps hps_tl
  have hplan : plan f fee price xs = hd := by
    unfold plan
    change (((parts xs).mergeSort le).head?.getD []) = hd
    simp [hs]
  rw [hplan]
  dsimp [le] at hlehd
  exact of_decide_eq_true hlehd

-- Exchange argument: if p < q, both optimalities force seek(A) ≤ seek(B).
private theorem seek_mono_of_cost_min
    (cA cB sA sB p q : Nat) (hpq : p < q)
    (hA : cA + q*sA ≤ cB + q*sB)
    (hB : cB + p*sB ≤ cA + p*sA) : sA ≤ sB := by
  have hsum : q*sA + p*sB ≤ q*sB + p*sA := by omega
  have hpsA : p*sA ≤ q*sA := Nat.mul_le_mul_right sA (Nat.le_of_lt hpq)
  have hpsB : p*sB ≤ q*sB := Nat.mul_le_mul_right sB (Nat.le_of_lt hpq)
  have hsub : q*sA - p*sA ≤ q*sB - p*sB := by omega
  rw [← Nat.mul_sub_right_distrib, ← Nat.mul_sub_right_distrib] at hsub
  exact Nat.le_of_mul_le_mul_left hsub (Nat.sub_pos_of_lt hpq)

theorem sound_HotPointReads : HotPointReads.Goal := by
  intro xs fee p q hpq
  have hA := plan_is_min write fee q xs (plan write fee p xs) (plan_mem write fee p xs)
  have hB := plan_is_min write fee p xs (plan write fee q xs) (plan_mem write fee q xs)
  dsimp [cost] at hA hB
  exact seek_mono_of_cost_min
    (charge write fee (plan write fee q xs))
    (charge write fee (plan write fee p xs))
    (seekWork (plan write fee q xs))
    (seekWork (plan write fee p xs)) p q hpq hA hB

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_HotPointReads
