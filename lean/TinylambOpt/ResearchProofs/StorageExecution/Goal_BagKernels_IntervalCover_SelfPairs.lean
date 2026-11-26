import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `IntervalCover.SelfPairs` — Count unordered equal-key pairs without materializing a
  self-join.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_SelfPairs

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem decide_toNat_eq (P : Prop) [Decidable P] : (decide P).toNat = if P then 1 else 0 := by
  by_cases h : P <;> simp [h]

private theorem bool_and_toNat (P Q : Prop) [Decidable P] [Decidable Q] :
    (if (decide P && decide Q) = true then 1 else 0) = if P ∧ Q then 1 else 0 := by
  by_cases hP : P <;> by_cases hQ : Q <;> simp [hP, hQ]

private theorem comb_succ (n : Nat) : comb (n+1) = comb n + n := by
  cases n with
  | zero => simp [comb]
  | succ m =>
      unfold comb
      rw [show (m + 1 + 1) * (m + 1 + 1 - 1) = (m + 1) * m + (m + 1) * 2 by
        simp [Nat.add_mul, Nat.mul_add, Nat.mul_comm]
        omega]
      rw [Nat.add_mul_div_right _ _ (by omega)]
      simp

private theorem count_range_shift (s n k : Nat) :
    ((List.range n).map (fun i => s + i)).count k = (decide (s ≤ k ∧ k < s + n)).toNat := by
  induction n with
  | zero =>
      have h : ¬ (s ≤ k ∧ k < s) := by omega
      simp [h]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.count_append, ih]
      simp only [List.map_cons, List.map_nil, List.count_singleton]
      simp only [decide_toNat_eq]
      by_cases hle : s ≤ k
      · by_cases hlt : k < s + n
        · have hlt' : k < s + (n+1) := by omega
          have hne : s + n ≠ k := by omega
          simp [hle, hlt, hlt', hne]
        · by_cases heq : s + n = k
          · have hlt' : k < s + (n+1) := by omega
            simp [hle, heq, hlt']
          · have hlt' : ¬ k < s + (n+1) := by omega
            simp [hle, hlt, heq, hlt']
      · have hlt : k < s + n := by omega
        have hlt' : k < s + (n+1) := by omega
        have hne : s + n ≠ k := by omega
        simp [hle, hlt, hlt', hne]

private theorem mass_eq_count (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass rs k = (bag.expand rs).count k := by
  induction rs with
  | nil => simp [bag]
  | cons r t ih =>
      obtain ⟨s, n⟩ := r
      simp only [bag, List.countP_cons, List.flatMap_cons, List.count_append]
      have hih : List.countP (fun x : Nat × Nat => decide (x.fst ≤ k) && decide (k < x.fst + x.snd)) t =
          (bag.expand t).count k := by simpa [bag] using ih
      rw [hih, count_range_shift]
      rw [bool_and_toNat]
      simp only [decide_toNat_eq]
      change (bag.expand t).count k + (if s ≤ k ∧ k < s + n then 1 else 0) =
        (if s ≤ k ∧ k < s + n then 1 else 0) + (bag.expand t).count k
      omega

private theorem peak_cons (x : Nat) (xs : List Nat) : peak (x :: xs) = max x (peak xs) := by
  cases xs <;> simp [peak, List.max?_cons]

private theorem le_peak_of_mem {xs : List Nat} {v : Nat} (h : v ∈ xs) : v ≤ peak xs := by
  induction xs with
  | nil => simp at h
  | cons x t ih =>
      rw [peak_cons]
      simp at h
      rcases h with rfl | h
      · exact Nat.le_max_left _ _
      · exact Nat.le_trans (ih h) (Nat.le_max_right _ _)

private theorem mem_expand_iff_cover (rs : List (Nat × Nat)) (k : Nat) :
    k ∈ bag.expand rs ↔ ∃ r ∈ rs, r.1 ≤ k ∧ k < r.1 + r.2 := by
  change k ∈ rs.flatMap (fun (r : Nat × Nat) => (List.range r.2).map (r.1 + ·)) ↔
    ∃ r ∈ rs, r.1 ≤ k ∧ k < r.1 + r.2
  rw [List.mem_flatMap]
  constructor
  · rintro ⟨⟨s, n⟩, hrs, hmem⟩
    rw [List.mem_map] at hmem
    obtain ⟨i, hi, hi_eq⟩ := hmem
    rw [List.mem_range] at hi
    refine ⟨(s, n), hrs, ?_, ?_⟩ <;> omega
  · rintro ⟨⟨s, n⟩, hrs, hsk, hksn⟩
    refine ⟨(s, n), hrs, ?_⟩
    rw [List.mem_map]
    refine ⟨k - s, ?_, ?_⟩
    · rw [List.mem_range]
      omega
    · omega

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

private theorem live_perm (rs : List (Nat × Nat)) :
    List.Perm (live bag rs) (bag.expand rs).eraseDups := by
  refine (List.perm_ext_iff_of_nodup ?_ ?_).2 ?_
  · change (((List.range (peak (rs.map (fun r : Nat × Nat => r.1 + r.2)))).eraseDups).filter
      (fun k => 0 < bag.mass rs k)).Nodup
    exact List.Pairwise.filter _ (nodup_eraseDups _)
  · exact nodup_eraseDups _
  · intro k
    constructor
    · intro hk
      change k ∈ ((List.range (peak (rs.map (fun r : Nat × Nat => r.1 + r.2)))).eraseDups).filter
        (fun k => 0 < bag.mass rs k) at hk
      simp only [List.mem_filter] at hk
      rcases hk with ⟨hkin, hpos⟩
      rw [List.mem_eraseDups] at hkin
      rw [List.mem_range] at hkin
      rw [List.mem_eraseDups]
      rw [mass_eq_count] at hpos
      have hpos' : 0 < (bag.expand rs).count k := by simpa using hpos
      exact List.count_pos_iff.mp hpos' 
    · intro hk
      rw [List.mem_eraseDups] at hk
      have hkin_expand : k ∈ bag.expand rs := hk
      rw [mem_expand_iff_cover] at hk
      rcases hk with ⟨⟨s, n⟩, hrs, hsk, hksn⟩
      have hle : s + n ≤ peak (rs.map (fun r : Nat × Nat => r.1 + r.2)) := by
        apply le_peak_of_mem
        rw [List.mem_map]
        exact ⟨(s, n), hrs, rfl⟩
      have hklt : k < peak (rs.map (fun r : Nat × Nat => r.1 + r.2)) := by omega
      change k ∈ ((List.range (peak (rs.map (fun r : Nat × Nat => r.1 + r.2)))).eraseDups).filter
        (fun k => 0 < bag.mass rs k)
      simp only [List.mem_filter, List.mem_eraseDups, List.mem_range]
      rw [mass_eq_count]
      have hpos' : decide (0 < (bag.expand rs).count k) = true := by
        simpa using (List.count_pos_iff.mpr hkin_expand)
      exact ⟨hklt, hpos'⟩

theorem sound_SelfPairs : IntervalCover.SelfPairs.Goal := by
  intro rs
  change ((live bag rs).map (fun k => comb (bag.mass rs k))).sum =
    ((bag.expand rs).zipIdx.map (fun (x : Nat × Nat) => ((bag.expand rs).drop (x.2 + 1)).count x.1)).sum
  have hp := (live_perm rs).map (fun k => comb (bag.mass rs k))
  have hsum := hp.sum_nat
  rw [hsum]
  have hmass : ((bag.expand rs).eraseDups.map (fun k => comb (bag.mass rs k))).sum =
      ((bag.expand rs).eraseDups.map (fun k => comb ((bag.expand rs).count k))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [mass_eq_count]
  rw [hmass]
  exact pair_identity (bag.expand rs)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_SelfPairs
