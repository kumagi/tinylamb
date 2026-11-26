import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `DictionaryAliases.OrderStatistic` — Find an order statistic with ties by cumulative
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

private theorem sum_indicator_eq_length {α : Type} (l : List α) (p : α → Bool) :
    (l.map (fun x => (p x).toNat)).sum = (l.filter p).length := by
  induction l with
  | nil => simp
  | cons a tl ih =>
      by_cases h : p a = true
      · simp [h, ih]
        omega
      · simp [h, ih]

private theorem zipIdx_countP (d : List Nat) (s i k : Nat) :
    (d.zipIdx s).countP (fun e => e.1 == k && e.2 == i) =
      if i < s then 0 else if d[i - s]? == some k then 1 else 0 := by
  induction d generalizing s with
  | nil =>
      simp [List.zipIdx]
  | cons a tl ih =>
      rw [List.zipIdx_cons]
      simp only [List.countP_cons]
      rw [ih (s + 1)]
      by_cases his : i < s
      · have his' : i < s + 1 := by omega
        have hsi : s ≠ i := by omega
        simp [his, his', hsi]
      · by_cases heq : i = s
        · subst i
          have hlt : ¬ s < s := by omega
          have hlt1 : s < s + 1 := by omega
          simp [hlt, hlt1]
        · have hilt : ¬ i < s + 1 := by omega
          have hsub : i - s = (i - (s + 1)) + 1 := by omega
          rw [hsub]
          have hsi : s ≠ i := by omega
          simp [his, hilt, hsi]

private theorem zipIdx_delta (d : List Nat) (s i k : Nat) :
    (((d.zipIdx s).filter (fun e => e.1 == k)).map (fun e => (if e.2 == i then 1 else 0 : Nat))).sum =
      if i < s then 0 else if d[i - s]? == some k then 1 else 0 := by
  have hmap : ((d.zipIdx s).filter (fun e => e.1 == k)).map (fun e => (if e.2 == i then 1 else 0 : Nat)) =
      ((d.zipIdx s).filter (fun e => e.1 == k)).map (fun e => ((e.2 == i).toNat)) := by
    apply List.map_congr_left
    intro e he
    cases e.2 == i <;> rfl
  rw [hmap]
  rw [sum_indicator_eq_length]
  rw [← List.countP_eq_length_filter]
  rw [List.countP_filter]
  simpa [Bool.and_comm] using (zipIdx_countP d s i k)

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => simp
  | cons a tl ih => simp [ih]

private theorem sum_map_add_nat {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a tl ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem mass_eq_count (d ids : List Nat) (k : Nat) :
    bag.mass (d, ids) k = (bag.expand (d, ids)).count k := by
  induction ids with
  | nil =>
      change (List.map (fun _ => (0 : Nat)) (List.filter (fun e => e.1 == k) d.zipIdx)).sum = 0
      exact sum_map_zero _
  | cons i tl ih =>
      unfold bag at ih ⊢
      simp only [List.count_cons]
      rw [sum_map_add_nat]
      have hdelta :
          (List.map (fun e => if i == e.2 then 1 else 0)
            (List.filter (fun e => e.1 == k) d.zipIdx)).sum =
            if d[i]? == some k then 1 else 0 := by
        have hz := zipIdx_delta d 0 i k
        simp only [Nat.sub_zero] at hz
        have hmap : (List.map (fun e => if i == e.2 then 1 else 0)
            (List.filter (fun e => e.1 == k) d.zipIdx)) =
            (List.map (fun e => if e.2 == i then 1 else 0)
            (List.filter (fun e => e.1 == k) d.zipIdx)) := by
          apply List.map_congr_left
          intro e he
          by_cases h : i = e.2
          · rw [h]
          · have h' : e.2 ≠ i := by intro hj; exact h hj.symm
            simp [h, h']
        rw [hmap]
        exact hz
      change (List.map (fun e => List.count e.2 tl) (List.filter (fun e => e.1 == k) d.zipIdx)).sum =
        List.count k (List.filterMap (fun i => d[i]?) tl) at ih
      rw [hdelta, ih]
      simp only [List.filterMap_cons]
      cases hdi : d[i]? with
      | none => simp
      | some v =>
          rw [List.count_cons]
          simp

private theorem sum_eraseDups_single_gen (l : List Nat) (p : Nat → Bool) (y x : Nat) :
    ((l.filter p).eraseDups.map
      (fun k => (if y == k then 1 else 0) * (decide (k ≤ x)).toNat)).sum =
      if y ∈ l.filter p then (decide (y ≤ x)).toNat else 0 := by
  induction l generalizing p with
  | nil => simp
  | cons a tl ih =>
      rw [List.filter_cons]
      by_cases hpa : p a = true
      · rw [ite_eq_left hpa, List.eraseDups_cons]
        simp only [List.map_cons, List.sum_cons]
        by_cases hay : a == y
        · have haeq : a = y := by simpa using hay
          subst a
          rw [List.filter_filter]
          have iht := ih (fun b => !(b == y) && p b)
          have hnot : ¬ y ∈ tl.filter (fun b => !(b == y) && p b) := by simp
          simp [hnot] at iht
          simp [iht]
        · have hne : a ≠ y := by
            intro h
            apply hay
            simp [h]
          have hya : y ≠ a := by intro h; exact hne h.symm
          rw [List.filter_filter]
          have iht := ih (fun b => !(b == a) && p b)
          have hmem : (y ∈ tl.filter (fun b => !(b == a) && p b)) ↔ y ∈ tl.filter p := by
            simp [hya]
          have hycons : (y ∈ a :: tl.filter p) ↔ y ∈ tl.filter p := by simp [hya]
          by_cases hytl : y ∈ tl.filter p
          · have hyc : y ∈ a :: tl.filter p := hycons.mpr hytl
            rw [ite_eq_left hyc]
            have hfilt : y ∈ List.filter (fun b => !(b == a) && p b) tl := hmem.mpr hytl
            simp [hfilt] at iht
            simp [hya, iht]
          · have hyc : ¬ y ∈ a :: tl.filter p := by intro h; exact hytl (hycons.mp h)
            rw [ite_eq_right hyc]
            have hfilt : ¬ y ∈ List.filter (fun b => !(b == a) && p b) tl := by
              intro h
              exact hytl (hmem.mp h)
            simp [hfilt] at iht
            simp [hya, iht]
      · rw [ite_eq_right hpa]
        exact ih p

private theorem filter_const_true (l : List Nat) : l.filter (fun _ => true) = l := by
  induction l with
  | nil => simp
  | cons a tl ih => simp [ih]

private theorem sum_eraseDups_single (l : List Nat) (y x : Nat) (hy : y ∈ l) :
    (l.eraseDups.map (fun k => (if y == k then 1 else 0) * (decide (k ≤ x)).toNat)).sum =
      (decide (y ≤ x)).toNat := by
  have h := sum_eraseDups_single_gen l (fun _ => true) y x
  rw [filter_const_true] at h
  simpa [hy] using h

private theorem sum_eraseDups_count (l xs : List Nat) (x : Nat)
    (hsub : ∀ y ∈ xs, y ∈ l) :
    (l.eraseDups.map (fun k => xs.count k * (decide (k ≤ x)).toNat)).sum =
      (xs.filter (fun y => y ≤ x)).length := by
  induction xs with
  | nil =>
      simpa using sum_map_zero l.eraseDups
  | cons y tl ih =>
      have hsub_tl : ∀ z ∈ tl, z ∈ l := by
        intro z hz
        exact hsub z (by simp [hz])
      have iht := ih hsub_tl
      have hy : y ∈ l := hsub y (by simp)
      have hsingle := sum_eraseDups_single l y x hy
      simp only [List.count_cons]
      simp only [Nat.add_mul]
      rw [sum_map_add_nat]
      rw [iht, hsingle]
      by_cases hyx : y ≤ x <;> simp [hyx]


private theorem expand_mem_keys (d ids : List Nat) {x : Nat} (hx : x ∈ bag.expand (d, ids)) :
    x ∈ d := by
  simp only [bag, List.mem_filterMap] at hx
  rcases hx with ⟨i, hi, hdi⟩
  have hsome : d[i]? = some x := by simpa using hdi
  have hex : ∃ h : i < d.length, d[i] = x := List.getElem?_eq_some_iff.mp hsome
  rw [← hex.choose_spec]
  exact List.getElem_mem hex.choose

private theorem below_eq_countP (d ids : List Nat) (x : Nat) :
    below bag (d, ids) x = ((bag.expand (d, ids)).filter (fun y => y ≤ x)).length := by
  unfold below weighted
  change (List.map (fun k => bag.mass (d, ids) k * (decide (k ≤ x)).toNat)
    (Prod.fst (d, ids)).eraseDups).sum = _
  have hmap : (List.map (fun k => bag.mass (d, ids) k * (decide (k ≤ x)).toNat) d.eraseDups) =
      (List.map (fun k => (bag.expand (d, ids)).count k * (decide (k ≤ x)).toNat) d.eraseDups) := by
    apply List.map_congr_left
    intro k hk
    rw [mass_eq_count]
  rw [hmap]
  exact sum_eraseDups_count d (bag.expand (d, ids)) x (by
    intro y hy
    exact expand_mem_keys d ids hy)

private theorem expand_subset_live (d ids : List Nat) {x : Nat} (hx : x ∈ bag.expand (d, ids)) :
    x ∈ live bag (d, ids) := by
  unfold live
  have hkeys : x ∈ bag.keys (d, ids) := expand_mem_keys d ids hx
  have hkeys2 : x ∈ (bag.keys (d, ids)).eraseDups := by
    simpa [bag] using hkeys
  simp only [List.mem_filter]
  constructor
  · exact hkeys2
  · rw [mass_eq_count]
    simp [List.count_pos_iff.mpr hx]

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

theorem sound_OrderStatistic : OrderStatistic.Goal := by
  intro p i
  rcases p with ⟨d, ids⟩
  unfold quantile
  simpa [below_eq_countP] using
    (quantile_aux (bag.expand (d, ids)) ((live bag (d, ids)).mergeSort) i
      (sort_pairwise (live bag (d, ids))) (by
        intro x hx
        exact List.mem_mergeSort.mpr (expand_subset_live d ids hx)))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_OrderStatistic
