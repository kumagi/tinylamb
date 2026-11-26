import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `ConstantExceptions.ParallelEncode` — Independent workers may force a boundary;
  global optimum is bounded by the sum of their plans.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_ParallelEncode

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.ConstantExceptions

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
  let le := fun a b => decide (objective f fee price a ≤ objective f fee price b)
  change ((parts xs).mergeSort le).head?.getD [] ∈ parts xs
  cases hm : (parts xs).mergeSort le with
  | nil =>
      have hlen := List.length_mergeSort (parts xs) (le := le)
      rw [hm] at hlen
      exact (parts_ne_nil xs (List.length_eq_zero_iff.mp hlen.symm)).elim
  | cons a t =>
      simp only [List.head?_cons, Option.getD_some]
      apply List.mem_mergeSort.mp
      rw [hm]
      exact List.mem_cons_self

private theorem plan_min (f : Writer) (fee price : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
    objective f fee price (plan f fee price xs) ≤ objective f fee price ps := by
  let le := fun a b => decide (objective f fee price a ≤ objective f fee price b)
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
      simpa [le] using hnat

-- Appending two partitions of consecutive lists is a partition of the concatenation.
private theorem parts_append {a b : List Nat} {pa pb : List (List Nat)}
    (ha : pa ∈ parts a) (hb : pb ∈ parts b) : pa ++ pb ∈ parts (a ++ b) := by
  induction a generalizing pa with
  | nil =>
      have hpa : pa = [] := by simpa [parts] using ha
      subst pa
      simpa using hb
  | cons x xs ih =>
      simp only [parts] at ha ⊢
      rcases List.mem_flatMap.mp ha with ⟨ps, hps, hstep⟩
      cases hpsnil : ps with
      | nil =>
          have hpa : pa = [[x]] := by simpa [hpsnil] using hstep
          subst pa
          have hpbparts : pb ∈ parts (xs ++ b) := by
            simpa [hpsnil] using ih (pa := []) (by simpa [hpsnil] using hps)
          apply List.mem_flatMap.mpr
          refine ⟨pb, hpbparts, ?_⟩
          cases pb with
          | nil => simp
          | cons q rest => simp
      | cons q rest =>
          simp only [hpsnil] at hps hstep
          have hps'parts : (q :: rest) ++ pb ∈ parts (xs ++ b) := ih hps
          apply List.mem_flatMap.mpr
          refine ⟨(q :: rest) ++ pb, hps'parts, ?_⟩
          rcases List.mem_cons.mp hstep with hpa | hpa
          · have hpa' : pa = [x] :: q :: rest := by simpa using hpa
            subst pa
            cases pb with
            | nil => simp
            | cons r rs => simp
          · have hpa' : pa = (x :: q) :: rest := by simpa using hpa
            subst pa
            cases pb with
            | nil => simp
            | cons r rs => simp

-- The optimal charge of a concatenation is at most the sum of the optimal charges.
theorem sound_ParallelEncode : ConstantExceptions.ParallelEncode.Goal := by
  intro a b fee
  let pa := plan write fee 0 a
  let pb := plan write fee 0 b
  have hmem : pa ++ pb ∈ parts (a ++ b) :=
    parts_append (plan_mem_parts write fee 0 a) (plan_mem_parts write fee 0 b)
  have hmin : objective write fee 0 (plan write fee 0 (a ++ b)) ≤
      objective write fee 0 (pa ++ pb) :=
    plan_min write fee 0 (a ++ b) (pa ++ pb) hmem
  change charge write fee (plan write fee 0 (a ++ b)) ≤
    charge write fee (plan write fee 0 a) + charge write fee (plan write fee 0 b)
  dsimp [pa, pb] at hmin ⊢
  simp only [objective] at hmin
  simp at hmin
  simpa [charge, List.map_append] using hmin

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_ParallelEncode
