import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `IntervalCover.OrderStatistic` — Find an order statistic with ties by cumulative
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover
open TinylambOpt.ResearchGoals.Compression.Wire

private abbrev leNat (a b : Nat) : Bool := decide (a ≤ b)

private theorem leNat_trans : ∀ a b c : Nat, leNat a b = true → leNat b c = true → leNat a c = true := by
  intro a b c hab hbc
  simp [leNat] at hab hbc ⊢
  omega

private theorem leNat_total : ∀ a b : Nat, (leNat a b || leNat b a) = true := by
  intro a b
  simp [leNat]
  omega

private theorem sort_pairwise (l : List Nat) : (l.mergeSort).Pairwise (fun a b => a ≤ b) := by
  exact (List.pairwise_mergeSort leNat_trans leNat_total l).imp (by
    intro a b h
    simpa [leNat] using h)

private theorem sort_perm_eq {l₁ l₂ : List Nat} (h : l₁.Perm l₂) : l₁.mergeSort = l₂.mergeSort := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
  · intro a b _ _ hab hba
    omega
  · exact sort_pairwise l₁
  · exact sort_pairwise l₂
  · exact (List.mergeSort_perm l₁ leNat).trans (h.trans (List.mergeSort_perm l₂ leNat).symm)

private theorem countP_le_of_imp {p q : Nat → Bool} (h : ∀ x, p x = true → q x = true) :
    ∀ l : List Nat, l.countP p ≤ l.countP q
  | [] => by simp
  | a :: l => by
      simp only [List.countP_cons]
      have hp : (if p a = true then 1 else 0) ≤ (if q a = true then 1 else 0) := by
        by_cases hpa : p a = true
        · have hqa : q a = true := h a hpa
          simp [hpa, hqa]
        · simp [hpa]
      have hi := countP_le_of_imp h l
      omega

private theorem rank_bounds {zs : List Nat} (hs : zs.Pairwise (fun a b => a ≤ b)) {i : Nat}
    (hi : i < zs.length) :
    zs.countP (fun y => y < zs[i]) ≤ i ∧ i < zs.countP (fun y => y ≤ zs[i]) := by
  induction zs generalizing i with
  | nil => simp at hi
  | cons a tl ih =>
      cases hs with
      | cons hforall htail =>
          by_cases hi0 : i = 0
          · subst i
            have hzero : tl.countP (fun y => y < a) = 0 := by
              apply List.countP_eq_zero.mpr
              intro y hy
              have hay : a ≤ y := hforall y hy
              simp [Nat.not_lt.mpr hay]
            constructor
            · simp [hzero]
            · simp
          · have hilen : i < tl.length + 1 := by simpa [List.length_cons] using hi
            have hitl : i - 1 < tl.length := by omega
            have ihres := ih htail hitl
            have hsome : some ((a :: tl)[i]) = some (tl[i - 1]) := by
              rw [← List.getElem?_eq_getElem hi]
              simp [List.getElem?_cons, hi0]
            have hget : (a :: tl)[i] = tl[i - 1] := Option.some.inj hsome
            have hmem : tl[i - 1] ∈ tl := List.getElem_mem hitl
            have hav : a ≤ tl[i - 1] := hforall _ hmem
            constructor
            · rw [hget, List.countP_cons]
              by_cases hv : a < tl[i - 1] <;> simp [hv] <;> omega
            · rw [hget, List.countP_cons]
              simp [hav]
              omega

private theorem find?_sorted {d : List Nat} (hs : d.Pairwise (fun a b => a ≤ b)) (pred : Nat → Bool)
    {v : Nat} (hv : v ∈ d) (hp : pred v = true)
    (hb : ∀ x, x ∈ d → x < v → pred x = false) :
    d.find? pred = some v := by
  induction d with
  | nil => simp at hv
  | cons a tl ih =>
      cases hs with
      | cons hforall htail =>
          by_cases hva : a = v
          · subst a
            simp [hp]
          · have hv' : v ∈ tl := by
              cases List.mem_cons.mp hv with
              | inl h => exact False.elim (hva h.symm)
              | inr h => exact h
            have hav : a < v := by
              have hle : a ≤ v := hforall v hv'
              omega
            have hpa : pred a = false := hb a (by simp) hav
            simp [hpa]
            exact ih htail hv' (by
              intro x hx hxv
              exact hb x (by simp [hx]) hxv)

private theorem quantile_aux (xs d : List Nat) (i : Nat)
    (hds : d.Pairwise (fun a b => a ≤ b))
    (hsub : ∀ x, x ∈ xs → x ∈ d) :
    d.find? (fun x => decide (i < (xs.filter (fun y => y ≤ x)).length)) = xs.mergeSort[i]? := by
  let zs := xs.mergeSort
  have hlen : zs.length = xs.length := by simp [zs]
  have hperm : zs.Perm xs := by simpa [zs] using List.mergeSort_perm xs leNat
  have hsp : zs.Pairwise (fun a b => a ≤ b) := by simpa [zs] using sort_pairwise xs
  by_cases hi : i < xs.length
  · have hizs : i < zs.length := by omega
    let v := zs[i]
    have hv_zs : v ∈ zs := by
      dsimp [v]
      exact List.getElem_mem hizs
    have hv_xs : v ∈ xs := (hperm.mem_iff).mp hv_zs
    have hv_d : v ∈ d := hsub v hv_xs
    have hrank := rank_bounds hsp hizs
    have hrank' : zs.countP (fun y => y < v) ≤ i ∧ i < zs.countP (fun y => y ≤ v) := by
      simpa [v] using hrank
    have hp : decide (i < (xs.filter (fun y => y ≤ v)).length) = true := by
      have hcp : xs.countP (fun y => y ≤ v) = zs.countP (fun y => y ≤ v) :=
        (List.Perm.countP_eq (fun y => y ≤ v) hperm).symm
      have hcnt : i < (xs.filter (fun y => y ≤ v)).length := by
        rw [← List.countP_eq_length_filter]
        rw [hcp]
        exact hrank'.2
      simp [hcnt]
    have hbefore : ∀ x, x ∈ d → x < v →
        decide (i < (xs.filter (fun y => y ≤ x)).length) = false := by
      intro x hx hxv
      have hcp : xs.countP (fun y => y ≤ x) = zs.countP (fun y => y ≤ x) :=
        (List.Perm.countP_eq (fun y => y ≤ x) hperm).symm
      have hmono : zs.countP (fun y => y ≤ x) ≤ zs.countP (fun y => y < v) := by
        apply countP_le_of_imp
        intro y hy
        simp at hy ⊢
        omega
      have hcnt : (xs.filter (fun y => y ≤ x)).length ≤ i := by
        rw [← List.countP_eq_length_filter]
        rw [hcp]
        omega
      have hnot : ¬ i < (xs.filter (fun y => y ≤ x)).length := by omega
      simp [hnot]
    have hf : d.find? (fun x => decide (i < (xs.filter (fun y => y ≤ x)).length)) = some v :=
      find?_sorted hds _ hv_d hp hbefore
    have hg' : xs.mergeSort[i]? = some v := by
      have hg : zs[i]? = some v := by
        dsimp [v]
        exact List.getElem?_eq_getElem hizs
      simpa [zs] using hg
    rw [hg']
    exact hf
  · have hgez : zs.length ≤ i := by omega
    have hnone : zs[i]? = none := List.getElem?_eq_none hgez
    have hnone' : xs.mergeSort[i]? = none := by simpa [zs] using hnone
    rw [hnone']
    apply List.find?_eq_none.mpr
    intro x hx
    have hbnd : (xs.filter (fun y => y ≤ x)).length ≤ xs.length := List.length_filter_le _ _
    have hnot : ¬ i < (xs.filter (fun y => y ≤ x)).length := by omega
    simp [hnot]

private theorem max?_getD_of_mem {xs : List Nat} {y : Nat} (hy : y ∈ xs) : y ≤ xs.max?.getD 0 := by
  induction xs with
  | nil => simp at hy
  | cons x tl ih =>
      cases List.mem_cons.mp hy with
      | inl h =>
          subst y
          cases hm : tl.max? with
          | none => simp [hm]
          | some m =>
              rw [List.max?_cons]
              simp [hm]
              exact Nat.le_max_left x m
      | inr h =>
          have ihy : y ≤ tl.max?.getD 0 := ih h
          cases hm : tl.max? with
          | none =>
              have hnil : tl = [] := by
                cases tl with
                | nil => rfl
                | cons b bt => simp [List.max?_cons] at hm
              subst tl
              simp at h
          | some m =>
              rw [List.max?_cons]
              simp [hm] at ihy ⊢
              omega

private theorem peak_le_cons (xs : List Nat) (a : Nat) : peak xs ≤ peak (a :: xs) := by
  unfold peak
  rw [List.max?_cons]
  simp
  cases hm : xs.max? with
  | none => simp
  | some m =>
      simp
      exact Nat.le_max_right a m

private theorem expand_mem_keys (rs : List (Nat × Nat)) {x : Nat} (hx : x ∈ bag.expand rs) :
    x ∈ bag.keys rs := by
  simp only [bag, List.mem_flatMap, List.mem_map] at hx
  rcases hx with ⟨r, hr, r', hr', hxr'⟩
  rcases r with ⟨s, n⟩
  have hrlt : r' < n := List.mem_range.mp hr'
  have hends : s + n ∈ rs.map (fun (s, n) => s + n) := List.mem_map.mpr ⟨(s, n), hr, rfl⟩
  have hpk : s + n ≤ peak (rs.map (fun (s, n) => s + n)) := max?_getD_of_mem hends
  subst x
  change s + r' ∈ List.range (peak (rs.map (fun (s, n) => s + n)))
  exact List.mem_range.mpr (by omega)

private theorem range_count (s n k : Nat) :
    ((List.range n).map (fun r => s + r)).count k = if s ≤ k && k < s + n then 1 else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.count_append, ih]
      simp only [List.map_singleton, List.count_cons, List.count_nil]
      by_cases hsk : s ≤ k
      · by_cases hkn : k < s + n
        · have hne : s + n ≠ k := by omega
          have hgoal : k < s + (n + 1) := by omega
          simp [hsk, hkn, hne, hgoal]
        · by_cases heq : s + n = k
          · subst k
            have hsk' : s ≤ s + n := by omega
            have hgoal : s < s + (n + 1) := by omega
            simp [hsk']
          · have hgt : s + n + 1 ≤ k := by omega
            have hgoal : ¬ k < s + (n + 1) := by omega
            simp [hsk, hkn, heq, hgoal]
      · have hne : s + n ≠ k := by omega
        simp [hsk, hne]

private theorem mass_count (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass rs k = (bag.expand rs).count k := by
  induction rs with
  | nil => simp [bag]
  | cons r tl ih =>
      rcases r with ⟨s, n⟩
      unfold bag at ih ⊢
      simp only [List.countP_cons, List.flatMap_cons]
      rw [List.count_append, ← ih, range_count]
      simp [Nat.add_comm]

private theorem eraseDups_range : ∀ n : Nat, (List.range n).eraseDups = List.range n
  | 0 => by simp
  | n + 1 => by
      rw [List.range_succ, List.eraseDups_append, eraseDups_range n]
      have hrm : ([n] : List Nat).removeAll (List.range n) = [n] := by
        simp [List.removeAll, List.mem_range]
      rw [hrm]
      simp [List.eraseDups_cons]

private theorem sum_map_zero (n : Nat) : ((List.range n).map (fun _ : Nat => 0)).sum = 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp

private theorem sum_range_eq_single (n y : Nat) (f : Nat → Nat) :
    ((List.range n).map (fun k => (if k == y then 1 else 0) * f k)).sum =
      if y < n then f y else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp only [List.map_singleton, List.sum_cons, List.sum_nil, Nat.add_zero]
      by_cases h : y = n
      · subst y
        simp
      · have hne : n ≠ y := by omega
        by_cases hlt : y < n
        · have hsucc : y < n.succ := Nat.lt_succ_of_lt hlt
          simp [hlt, hsucc, hne]
        · have hnlt : ¬ y < n.succ := by omega
          simp [hlt, hnlt, hne]

private theorem sum_map_add_nat (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a tl ih =>
      simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_range_count (n : Nat) (xs : List Nat) (x : Nat)
    (hpeak : ∀ y ∈ xs, y < n) :
    ((List.range n).map (fun k => xs.count k * (decide (k ≤ x)).toNat)).sum =
      ((xs.filter (fun y => y ≤ x)).length) := by
  induction xs with
  | nil =>
      simpa using sum_map_zero n
  | cons y tl ih =>
      have hpeak_tl : ∀ z ∈ tl, z < n := by
        intro z hz
        exact hpeak z (by simp [hz])
      have ih' := ih hpeak_tl
      have hpoint : (List.range n).map (fun k => (y :: tl).count k * (decide (k ≤ x)).toNat) =
          (List.range n).map (fun k =>
            tl.count k * (decide (k ≤ x)).toNat +
              (if k == y then 1 else 0) * (decide (k ≤ x)).toNat) := by
        apply List.map_congr_left
        intro k hk
        rw [List.count_cons]
        by_cases hyk : y = k
        · subst k
          simp [Nat.add_mul]
        · have hky : k ≠ y := by intro h; exact hyk h.symm
          simp [hyk, hky]
      have hmain : ((List.range n).map (fun k => (y :: tl).count k * (decide (k ≤ x)).toNat)).sum =
          ((List.range n).map (fun k => tl.count k * (decide (k ≤ x)).toNat)).sum +
          ((List.range n).map (fun k => (if k == y then 1 else 0) * (decide (k ≤ x)).toNat)).sum := by
        rw [hpoint, sum_map_add_nat]
      have hsingle : ((List.range n).map (fun k => (if k == y then 1 else 0) * (decide (k ≤ x)).toNat)).sum =
          (if y ≤ x then 1 else 0) := by
        have hylt : y < n := hpeak y (by simp)
        have hs := sum_range_eq_single n y (fun k => (decide (k ≤ x)).toNat)
        by_cases hyx : y ≤ x
        · simpa [hylt, hyx] using hs
        · simpa [hylt, hyx] using hs
      rw [hmain, ih', hsingle]
      by_cases hyx : y ≤ x <;> simp [hyx] <;> omega

private theorem below_eq_countP (rs : List (Nat × Nat)) (x : Nat) :
    below bag rs x = ((bag.expand rs).filter (fun y => y ≤ x)).length := by
  unfold below weighted
  have hkeys : (bag.keys rs).eraseDups = List.range (peak (rs.map (fun (s, n) => s + n))) := by
    change (List.range (peak (rs.map (fun (s, n) => s + n)))).eraseDups = _
    rw [eraseDups_range]
  rw [hkeys]
  simp only [mass_count]
  apply sum_range_count
  intro y hy
  have hykeys : y ∈ bag.keys rs := expand_mem_keys rs hy
  simpa [bag, List.mem_range] using hykeys

private theorem expand_subset_live (rs : List (Nat × Nat)) {x : Nat} (hx : x ∈ bag.expand rs) :
    x ∈ live bag rs := by
  unfold live
  have hkeys : x ∈ bag.keys rs := expand_mem_keys rs hx
  have hkeys2 : x ∈ (bag.keys rs).eraseDups := by
    simpa [bag, eraseDups_range] using hkeys
  simp only [List.mem_filter]
  constructor
  · exact hkeys2
  · rw [mass_count]
    simp [List.count_pos_iff.mpr hx]

theorem sound_OrderStatistic : OrderStatistic.Goal := by
  intro rs i
  unfold quantile
  simpa [below_eq_countP] using
    (quantile_aux (bag.expand rs) ((live bag rs).mergeSort) i
      (sort_pairwise (live bag rs)) (by
        intro x hx
        exact List.mem_mergeSort.mpr (expand_subset_live rs hx)))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_OrderStatistic
