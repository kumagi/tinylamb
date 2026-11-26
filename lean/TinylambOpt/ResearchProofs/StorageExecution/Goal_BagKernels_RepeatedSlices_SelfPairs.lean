import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `RepeatedSlices.SelfPairs` — Count unordered equal-key pairs without materializing a
  self-join.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_SelfPairs

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RepeatedSlices

private theorem comb_succ (n : Nat) : comb (n + 1) = comb n + n := by
  cases n with
  | zero => simp [comb]
  | succ m =>
      unfold comb
      rw [show (m + 1 + 1) * (m + 1 + 1 - 1) = (m + 1) * m + (m + 1) * 2 by
        simp [Nat.add_mul, Nat.mul_add, Nat.mul_comm]
        omega]
      rw [Nat.add_mul_div_right _ _ (by omega)]
      simp

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List Nat, l.length = n → l.eraseDups.Nodup
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen
    cases l with
    | nil => simp
    | cons a t =>
        rw [List.eraseDups_cons]
        apply List.nodup_cons.mpr
        constructor
        · intro ha
          have hfa : a ∈ t.filter (fun b => ! b == a) := List.mem_eraseDups.mp ha
          simp at hfa
        · have hle : (t.filter (fun b => ! b == a)).length ≤ t.length :=
            List.length_filter_le (fun b => ! b == a) t
          have hlt : (t.filter (fun b => ! b == a)).length < n := by
            simp at hlen
            omega
          exact ih _ hlt _ rfl
  exact H l rfl

private theorem count_eq_one_of_mem_nodup {l : List Nat} {k : Nat}
    (hn : l.Nodup) (hk : k ∈ l) : l.count k = 1 := by
  induction l with
  | nil => cases hk
  | cons b bs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.mem_cons] at hk
      rw [List.count_cons]
      by_cases hbk : b = k
      · subst k
        simp [List.count_eq_zero_of_not_mem hn.1]
      · have hk' : k ∈ bs := by
          cases hk with
          | inl h => exact (hbk h.symm).elim
          | inr h => exact h
        have hbeq : (b == k) = false := by simp [hbk]
        simp [hbeq, ih hn.2 hk']

private theorem count_eraseDups (l : List Nat) (k : Nat) :
    l.eraseDups.count k = if k ∈ l then 1 else 0 := by
  by_cases h : k ∈ l
  · have hmem : k ∈ l.eraseDups := by simpa [List.mem_eraseDups] using h
    simp [h, count_eq_one_of_mem_nodup (nodup_eraseDups l) hmem]
  · have hmem : k ∉ l.eraseDups := by simpa [List.mem_eraseDups] using h
    simp [h, List.count_eq_zero_of_not_mem hmem]

private theorem rhs_cons (a : Nat) (t : List Nat) :
    (((a :: t).zipIdx.map (fun (x : Nat × Nat) => ((a :: t).drop (x.2 + 1)).count x.1)).sum) =
      t.count a + ((t.zipIdx.map (fun (x : Nat × Nat) => (t.drop (x.2 + 1)).count x.1)).sum) := by
  rw [List.zipIdx_cons']
  simp only [List.map_cons, List.sum_cons]
  have hhead : (a :: t).drop 1 = t := by simp
  rw [hhead]
  congr 1
  apply congrArg List.sum
  rw [List.map_map]
  apply List.map_congr_left
  rintro ⟨y, i⟩ hx
  simp only [Function.comp_apply, Prod.map]
  rw [show (a :: t).drop (i + 1 + 1) = t.drop (i + 1) by
    rw [show i + 1 + 1 = (i + 1) + 1 by omega, List.drop_succ_cons]]
  simp

private theorem eraseDups_filter_perm (a : Nat) (t : List Nat) :
    List.Perm ((t.filter (fun b => ! b == a)).eraseDups ++ (if a ∈ t then [a] else [])) t.eraseDups := by
  apply List.perm_iff_count.mpr
  intro k
  rw [List.count_append]
  by_cases hat : a ∈ t
  · by_cases hka : k = a
    · subst k
      have hnot : a ∉ (t.filter (fun b => ! b == a)).eraseDups := by
        rw [List.mem_eraseDups]
        simp
      simp [hat, count_eraseDups]
    · have hka' : a ≠ k := fun h => hka h.symm
      have hkbeq : (k == a) = false := by simp [hka]
      have hf : k ∈ (t.filter (fun b => ! b == a)).eraseDups ↔ k ∈ t := by
        rw [List.mem_eraseDups]
        simp [hkbeq]
      simp [hat, count_eraseDups, hka, hka']
  · by_cases hka : k = a
    · subst k
      have hnot : a ∉ (t.filter (fun b => ! b == a)).eraseDups := by
        rw [List.mem_eraseDups]
        simp
      simp [hat, count_eraseDups]
    · have hka' : a ≠ k := fun h => hka h.symm
      have hkbeq : (k == a) = false := by simp [hka]
      have hf : k ∈ (t.filter (fun b => ! b == a)).eraseDups ↔ k ∈ t := by
        rw [List.mem_eraseDups]
        simp [hkbeq]
      simp [hat, count_eraseDups, hka]

private theorem pair_identity (xs : List Nat) :
    ((xs.eraseDups.map (fun k => comb (xs.count k))).sum) =
      ((xs.zipIdx.map (fun (x : Nat × Nat) => (xs.drop (x.2 + 1)).count x.1)).sum) := by
  induction xs with
  | nil => simp [comb]
  | cons a t ih =>
      rw [rhs_cons a t]
      rw [List.eraseDups_cons]
      simp only [List.map_cons, List.sum_cons]
      have hcnta : (a :: t).count a = t.count a + 1 := by simp
      rw [hcnta, comb_succ]
      have hmap :
          ((t.filter (fun b => ! b == a)).eraseDups.map (fun k => comb ((a :: t).count k))).sum =
          ((t.filter (fun b => ! b == a)).eraseDups.map (fun k => comb (t.count k))).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k hk
        have hkf : k ∈ t.filter (fun b => ! b == a) := List.mem_eraseDups.mp hk
        rw [List.mem_filter] at hkf
        rcases hkf with ⟨hkt, hkbeq⟩
        have hkne : k ≠ a := by
          intro he
          subst k
          simp at hkbeq
        rw [List.count_cons_of_ne hkne.symm]
      rw [hmap]
      have hp := (eraseDups_filter_perm a t).map (fun k => comb (t.count k))
      have hsum : ((t.eraseDups.map (fun k => comb (t.count k))).sum) =
          (((t.filter (fun b => ! b == a)).eraseDups.map (fun k => comb (t.count k))).sum) +
            (if a ∈ t then comb (t.count a) else 0) := by
        have hs := hp.sum_nat
        by_cases hat : a ∈ t
        · simp [hat] at hs
          simpa [hat] using hs.symm
        · simp [hat] at hs
          simpa [hat] using hs.symm
      rw [← ih]
      rw [hsum]
      have hcomb : comb (t.count a) = if a ∈ t then comb (t.count a) else 0 := by
        by_cases h : a ∈ t
        · simp [h]
        · simp [h, List.count_eq_zero_of_not_mem h, comb]
      rw [← hcomb]
      omega

-- RepeatedSlices-specific kernels.
private def massFrom (i : Nat) (xs : List Nat) (k : Nat) : Nat :=
  (((xs.zipIdx i).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum

private theorem massFrom_zero (xs : List Nat) (k : Nat) : massFrom 0 xs k = bag.mass xs k := by
  rfl

private theorem massFrom_add (i : Nat) (xs : List Nat) (k : Nat) :
    massFrom i xs k = massFrom 0 xs k + i * xs.count k := by
  induction xs generalizing i with
  | nil => simp [massFrom]
  | cons a t ih =>
      rw [show massFrom i (a :: t) k = (if a == k then i + 1 else 0) + massFrom (i + 1) t k by
        by_cases h : a == k <;> simp [massFrom, List.zipIdx_cons, h]]
      rw [ih (i + 1)]
      rw [show massFrom 0 (a :: t) k = (if a == k then 1 else 0) + massFrom 1 t k by
        by_cases h : a == k <;> simp [massFrom, List.zipIdx_cons, h]]
      rw [ih 1]
      simp only [List.count_cons]
      simp only [Nat.add_mul, Nat.mul_add]
      by_cases h : a == k <;> simp [h] <;> omega

private theorem mass_cons (a : Nat) (t : List Nat) (k : Nat) :
    bag.mass (a :: t) k = (if a == k then 1 else 0) + bag.mass t k + t.count k := by
  rw [← massFrom_zero (a :: t) k]
  rw [show massFrom 0 (a :: t) k = (if a == k then 1 else 0) + massFrom 1 t k by
    by_cases h : a == k <;> simp [massFrom, List.zipIdx_cons, h]]
  rw [show massFrom 1 t k = massFrom 0 t k + t.count k by
    simpa using massFrom_add 1 t k]
  rw [massFrom_zero]
  omega

private theorem expand_cons (a : Nat) (t : List Nat) :
    bag.expand (a :: t) = (a :: t) ++ bag.expand t := by
  change (List.range ((a :: t).length)).flatMap (fun i => (a :: t).drop i) =
    (a :: t) ++ (List.range t.length).flatMap (fun i => t.drop i)
  simp only [List.length_cons]
  rw [List.range_succ_eq_map]
  rw [List.flatMap_cons]
  simp only [List.drop_zero]
  congr 1
  rw [List.flatMap_map]
  simp only [Nat.succ_eq_add_one, List.drop_succ_cons]

private theorem mass_eq_count (xs : List Nat) (k : Nat) :
    bag.mass xs k = (bag.expand xs).count k := by
  induction xs with
  | nil => simp [bag]
  | cons a t ih =>
      rw [expand_cons, List.count_append, mass_cons, ih]
      simp only [List.count_cons]
      omega

private theorem mem_expand (xs : List Nat) (k : Nat) : k ∈ bag.expand xs ↔ k ∈ xs := by
  induction xs with
  | nil => simp [bag]
  | cons a t ih =>
      rw [expand_cons, List.mem_append, List.mem_cons, ih]
      constructor
      · intro h
        rcases h with h | h
        · exact h
        · exact Or.inr h
      · intro h
        exact Or.inl h

private theorem live_perm (xs : List Nat) :
    List.Perm (live bag xs) (bag.expand xs).eraseDups := by
  change List.Perm (xs.eraseDups.filter (fun k => 0 < bag.mass xs k))
    (bag.expand xs).eraseDups
  refine (List.perm_ext_iff_of_nodup ?_ ?_).2 ?_
  · exact List.Pairwise.filter _ (nodup_eraseDups xs)
  · exact nodup_eraseDups _
  · intro k
    constructor
    · intro hk
      rw [List.mem_filter] at hk
      rcases hk with ⟨hkxs, hpos⟩
      rw [List.mem_eraseDups]
      rw [List.mem_eraseDups] at hkxs
      rw [mass_eq_count] at hpos
      have hpos' : 0 < (bag.expand xs).count k := by simpa using hpos
      exact List.count_pos_iff.mp hpos'
    · intro hk
      rw [List.mem_eraseDups] at hk
      have hkxs : k ∈ xs := (mem_expand xs k).mp hk
      simp only [List.mem_filter, List.mem_eraseDups]
      rw [mass_eq_count]
      have hpos : 0 < (bag.expand xs).count k := List.count_pos_iff.mpr hk
      exact ⟨hkxs, by simpa using hpos⟩

theorem sound_SelfPairs : RepeatedSlices.SelfPairs.Goal := by
  intro xs
  change ((live bag xs).map (fun k => comb (bag.mass xs k))).sum =
    ((bag.expand xs).zipIdx.map (fun (x : Nat × Nat) =>
      ((bag.expand xs).drop (x.2 + 1)).count x.1)).sum
  have hp := (live_perm xs).map (fun k => comb (bag.mass xs k))
  rw [hp.sum_nat]
  have hmass : ((bag.expand xs).eraseDups.map (fun k => comb (bag.mass xs k))).sum =
      ((bag.expand xs).eraseDups.map (fun k => comb ((bag.expand xs).count k))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [mass_eq_count]
  rw [hmass]
  exact pair_identity (bag.expand xs)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_SelfPairs
