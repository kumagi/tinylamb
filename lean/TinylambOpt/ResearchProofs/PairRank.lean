import TinylambOpt.ResearchGoals.PairRank

namespace TinylambOpt.ResearchProofs.PairRank

open TinylambOpt.ResearchGoals.PairRank

private def numLE (t : Nat) (xs ys : List Nat) : Nat :=
  (xs.flatMap fun x => ys.filter fun y => x + y ≤ t).length

private theorem filter_length_mono {α : Type} (p q : α → Bool) (xs : List α)
    (h : ∀ a, p a = true → q a = true) :
    (xs.filter p).length ≤ (xs.filter q).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · have hq : q x = true := h x hp
        simp [List.filter_cons, hp, hq]
        omega
      · by_cases hq : q x = true
        · simp [List.filter_cons, hp, hq]
          omega
        · simpa [List.filter_cons, hp, hq] using ih

private theorem numLE_mono {t₁ t₂ : Nat} (h : t₁ ≤ t₂) (xs ys : List Nat) :
    numLE t₁ xs ys ≤ numLE t₂ xs ys := by
  unfold numLE
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.flatMap_cons, List.length_append]
      have hf := filter_length_mono (fun y : Nat => x + y ≤ t₁) (fun y : Nat => x + y ≤ t₂) ys
        (by intro y hy; exact decide_eq_true (by
          have : x + y ≤ t₁ := of_decide_eq_true hy
          omega))
      omega

private theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) {xs : List α}
    (h : ∀ x ∈ xs, p x = false) : xs.filter p = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = false := h x (by simp)
      have ht : ∀ z ∈ xs, p z = false := by intro z hz; exact h z (by simp [hz])
      simp [List.filter_cons, hx, ih ht]

private theorem filter_eq_self_of_le (t x : Nat) {ys : List Nat}
    (hall : ∀ y ∈ ys, x + y ≤ t) :
    ys.filter (fun y : Nat => x + y ≤ t) = ys := by
  apply List.filter_eq_self.mpr
  intro y hy
  exact decide_eq_true (hall y hy)

private theorem sum_map_zero (xs : List Nat) : (xs.map fun _ => 0).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simpa using ih

private theorem numLE_nil_right (t : Nat) : ∀ xs : List Nat, numLE t xs [] = 0 := by
  intro xs
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [numLE, ih, sum_map_zero]

private theorem numLE_cons_all (t x : Nat) (xs ys : List Nat)
    (hall : ∀ y ∈ ys, x + y ≤ t) :
    numLE t (x :: xs) ys = ys.length + numLE t xs ys := by
  unfold numLE
  simp only [List.flatMap_cons, List.length_append]
  rw [filter_eq_self_of_le t x hall]

private theorem numLE_drop_head_list (t y : Nat) (ys xs : List Nat)
    (hxall : ∀ z ∈ xs, ¬ z + y ≤ t) :
    (xs.flatMap fun z => (y :: ys).filter fun w : Nat => z + w ≤ t).length =
      (xs.flatMap fun z => ys.filter fun w : Nat => z + w ≤ t).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.flatMap_cons, List.length_append]
      have hx : (y :: ys).filter (fun w : Nat => x + w ≤ t) =
          ys.filter (fun w : Nat => x + w ≤ t) := by
        simp [List.filter_cons, decide_eq_false (hxall x (by simp))]
      have htail : ∀ z ∈ xs, ¬ z + y ≤ t := by
        intro z hz
        exact hxall z (by simp [hz])
      rw [hx, ih htail]

private def Asc (xs : List Nat) : Prop := xs.Pairwise (fun a b => a ≤ b)
private def Desc (ys : List Nat) : Prop := ys.Pairwise (fun a b => b ≤ a)

private theorem sorted_mergeSort (xs : List Nat) : Asc xs.mergeSort := by
  have h := List.pairwise_mergeSort (le := fun a b : Nat => a ≤ b)
    (by intro a b c hab hbc
        have hab' : a ≤ b := of_decide_eq_true hab
        have hbc' : b ≤ c := of_decide_eq_true hbc
        exact decide_eq_true (by omega))
    (by intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have h' : b ≤ a := by omega
          simp [h']) xs
  exact h.imp (by intro a b hab; exact of_decide_eq_true hab)

private theorem sorted_mergeSort_reverse (xs : List Nat) : Desc xs.mergeSort.reverse := by
  rw [Desc, List.pairwise_reverse]
  exact sorted_mergeSort xs

private theorem countLE_eq_numLE (t : Nat) (xs ys : List Nat)
    (hAsc : Asc xs) (hDesc : Desc ys) :
    countLE t (xs.length + ys.length) xs ys ys.length = numLE t xs ys := by
  cases xs with
  | nil =>
      cases ys with
      | nil => simp [countLE, numLE]
      | cons y ys => simp [countLE, numLE]
  | cons x xs' =>
      cases ys with
      | nil => simp [countLE, numLE_nil_right]
      | cons y ys' =>
          have hAscTail : Asc xs' := List.Pairwise.of_cons hAsc
          have hDescTail : Desc ys' := List.Pairwise.of_cons hDesc
          have hx_le : ∀ z ∈ xs', x ≤ z := (List.pairwise_cons.mp hAsc).1
          have hy_le : ∀ z ∈ ys', z ≤ y := (List.pairwise_cons.mp hDesc).1
          by_cases hxy : x + y ≤ t
          · have hall : ∀ z ∈ y :: ys', x + z ≤ t := by
              intro z hz
              simp at hz
              rcases hz with rfl | hz
              · exact hxy
              · have := hy_le z hz
                omega
            have hnum := numLE_cons_all t x xs' (y :: ys') hall
            have hrec := countLE_eq_numLE t xs' (y :: ys') hAscTail hDesc
            have hrec' : countLE t (xs'.length + 1 + ys'.length) xs' (y :: ys') (ys'.length + 1) =
                numLE t xs' (y :: ys') := by
              simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hrec
            simp [countLE, hxy, hnum, hrec']
          · have hxall : ∀ z ∈ x :: xs', ¬ z + y ≤ t := by
              intro z hz
              simp at hz
              rcases hz with rfl | hz
              · exact hxy
              · have := hx_le z hz
                omega
            have hdrop_tail := numLE_drop_head_list t y ys' xs' (by
              intro z hz
              exact hxall z (by simp [hz]))
            have hdrop : numLE t (x :: xs') (y :: ys') = numLE t (x :: xs') ys' := by
              unfold numLE
              simp only [List.flatMap_cons, List.length_append]
              have hx : (y :: ys').filter (fun w : Nat => x + w ≤ t) =
                  ys'.filter (fun w : Nat => x + w ≤ t) := by
                simp [List.filter_cons, decide_eq_false (hxall x (by simp))]
              rw [hx, hdrop_tail]
            have hrec := countLE_eq_numLE t (x :: xs') ys' hAsc hDescTail
            have hrec' : countLE t (xs'.length + 1 + ys'.length) (x :: xs') ys' ys'.length =
                numLE t (x :: xs') ys' := by
              simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hrec
            simp [countLE, hxy, hdrop, hrec']
termination_by xs.length + ys.length

private def IsAnswer (count : Nat → Nat) (k t : Nat) : Prop :=
  k < count t ∧ ∀ u < t, count u ≤ k

private theorem pow_succ_two (n : Nat) : 2 ^ (n + 1) = 2 * 2 ^ n := by
  rw [show n + 1 = Nat.succ n by omega]
  rw [Nat.pow_succ]
  omega

private theorem search_is_answer (count : Nat → Nat)
    (hmono : ∀ a b : Nat, a ≤ b → count a ≤ count b)
    (k : Nat) : ∀ fuel lo hi : Nat,
      k < count hi → (∀ u < lo, count u ≤ k) → lo ≤ hi →
      hi < lo + 2 ^ fuel → IsAnswer count k (search count k fuel lo hi) := by
  intro fuel
  induction fuel with
  | zero =>
      intro lo hi hhi hlo hle hwidth
      have hih : hi = lo := by
        have hw : hi < lo + 1 := by simpa using hwidth
        omega
      subst hi
      exact ⟨hhi, hlo⟩
  | succ fuel ih =>
      intro lo hi hhi hlo hle hwidth
      by_cases hlohi : lo = hi
      · subst hi
        simpa [search] using (show IsAnswer count k lo from ⟨hhi, hlo⟩)
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hmid_lo : lo ≤ mid := by dsimp [mid]; omega
        have hmid_hi : mid < hi := by dsimp [mid]; omega
        rw [pow_succ_two] at hwidth
        by_cases hbranch : k < count mid
        · have hw : mid < lo + 2 ^ fuel := by
            dsimp [mid] at *
            omega
          have hres := ih lo mid hbranch hlo hmid_lo hw
          simpa [search, hlohi, mid, hbranch] using hres
        · have hcntmid : count mid ≤ k := by omega
          have hlo' : ∀ u < mid + 1, count u ≤ k := by
            intro u hu
            have hum : u ≤ mid := by omega
            exact Nat.le_trans (hmono u mid hum) hcntmid
          have hw : hi < mid + 1 + 2 ^ fuel := by
            dsimp [mid] at *
            omega
          have hres := ih (mid + 1) hi hhi hlo' (by omega) hw
          simpa [search, hlohi, mid, hbranch] using hres

private theorem isAnswer_unique (count : Nat → Nat) (k t₁ t₂ : Nat)
    (h₁ : IsAnswer count k t₁) (h₂ : IsAnswer count k t₂) : t₁ = t₂ := by
  unfold IsAnswer at h₁ h₂
  by_cases h : t₁ < t₂
  · have := h₂.2 t₁ h
    omega
  · by_cases h' : t₂ < t₁
    · have := h₁.2 t₂ h'
      omega
    · omega

private theorem filter_eq_nil_of_not_le {ys : List Nat} {u : Nat}
    (h : ∀ z ∈ ys, ¬ z ≤ u) : ys.filter (fun z : Nat => z ≤ u) = [] := by
  induction ys with
  | nil => rfl
  | cons z zs ih =>
      have hz' : ¬ z ≤ u := h z (by simp)
      have hz : decide (z ≤ u) = false := decide_eq_false hz'
      have ht : ∀ w ∈ zs, ¬ w ≤ u := by intro w hw; exact h w (by simp [hw])
      have htail := ih ht
      simp [List.filter_cons, hz, htail]

private theorem kth_is_answer (L : List Nat) (hsort : Asc L) (k : Nat) (hk : k < L.length) :
    IsAnswer (fun t => (L.filter fun s : Nat => s ≤ t).length) k L[k] := by
  constructor
  · change k < (L.filter (fun s : Nat => s ≤ L[k])).length
    have htake : (L.take (k + 1)).filter (fun s : Nat => s ≤ L[k]) = L.take (k + 1) := by
      apply List.filter_eq_self.mpr
      intro s hs
      rw [List.mem_iff_getElem] at hs
      rcases hs with ⟨i, hi, hs_eq⟩
      rw [← hs_eq]
      rw [List.getElem_take (xs := L) (j := k + 1) (i := i)]
      have hi_lim : i < k + 1 := by
        have ht := hi
        simp [List.length_take] at ht
        omega
      have hiL : i < L.length := by
        have ht := hi
        simp [List.length_take] at ht
        omega
      by_cases hik : i = k
      · subst i
        simp
      · have hiklt : i < k := by omega
        exact decide_eq_true (hsort.rel_getElem_of_lt hiL hk hiklt)
    have hsub : ((L.take (k + 1)).filter (fun s : Nat => s ≤ L[k])).Sublist
        (L.filter (fun s : Nat => s ≤ L[k])) :=
      List.Sublist.filter _ (List.take_sublist (k + 1) L)
    have hlen := hsub.length_le
    rw [htake] at hlen
    have htake_len : (L.take (k + 1)).length = k + 1 := by
      rw [List.length_take]
      exact Nat.min_eq_left (by omega)
    have hlen' : k + 1 ≤ (L.filter (fun s : Nat => s ≤ L[k])).length := by
      rw [← htake_len]
      exact hlen
    omega
  · intro u hu
    change (L.filter (fun s : Nat => s ≤ u)).length ≤ k
    have hsplit : L.filter (fun s : Nat => s ≤ u) =
        (L.take k).filter (fun s : Nat => s ≤ u) ++ (L.drop k).filter (fun s : Nat => s ≤ u) := by
      conv => lhs; rw [← List.take_append_drop k L]
      rw [List.filter_append]
    have hdrop_nil : (L.drop k).filter (fun s : Nat => s ≤ u) = [] := by
      apply filter_eq_nil_of_not_le
      intro z hz
      rw [List.mem_iff_getElem] at hz
      rcases hz with ⟨i, hi, hzEq⟩
      rw [← hzEq]
      rw [List.getElem_drop (xs := L) (i := k) (j := i)]
      have hil : k + i < L.length := by
        have hilen := List.length_drop (l := L) (i := k)
        have hklen : k ≤ L.length := by omega
        omega
      have hle : L[k] ≤ L[k + i] := by
        by_cases hi0 : i = 0
        · subst i
          simp
        · exact hsort.rel_getElem_of_lt hk hil (by omega)
      have hlt : u < L[k + i] := by omega
      have hnot : ¬ L[k + i] ≤ u := Nat.not_le_of_gt hlt
      exact hnot
    rw [hsplit, hdrop_nil]
    simp
    have hlen : ((L.take k).filter (fun s : Nat => s ≤ u)).length ≤ (L.take k).length :=
      List.length_filter_le _ _
    have htk : (L.take k).length = k := by
      rw [List.length_take]
      exact Nat.min_eq_left (by omega)
    omega

private theorem perm_flatMap {α β : Type} (f : α → List β) {l₁ l₂ : List α}
    (p : l₁.Perm l₂) : (l₁.flatMap f).Perm (l₂.flatMap f) := by
  induction p with
  | nil => rfl
  | cons a p ih =>
      simpa using ih.append_left (f a)
  | swap a b l =>
      have h : (f b ++ f a).Perm (f a ++ f b) := (List.perm_append_comm (l₁ := f b) (l₂ := f a))
      simpa using h.append_right (l.flatMap f)
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem perm_flatMap_fun {α β : Type} {l : List α} (f g : α → List β)
    (h : ∀ a ∈ l, (f a).Perm (g a)) : (l.flatMap f).Perm (l.flatMap g) := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.flatMap_cons]
      have ha := h a (by simp)
      have ht : ∀ z ∈ l, (f z).Perm (g z) := by intro z hz; exact h z (by simp [hz])
      exact (ha.append_right (l.flatMap f)).trans ((ih ht).append_left (g a))

private theorem perm_cons_append_singleton (x : Nat) : ∀ ys : List Nat, (x :: ys).Perm (ys ++ [x]) := by
  intro ys
  induction ys with
  | nil => rfl
  | cons y ys ih =>
      have hswap : (x :: y :: ys).Perm (y :: x :: ys) := List.Perm.swap y x ys
      have htail : (y :: x :: ys).Perm (y :: (ys ++ [x])) := ih.cons y
      exact hswap.trans htail

private theorem perm_reverse (l : List Nat) : l.Perm l.reverse := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have h1 : (x :: xs).Perm (x :: xs.reverse) := ih.cons x
      have h2 : (x :: xs.reverse).Perm (xs.reverse ++ [x]) := perm_cons_append_singleton x xs.reverse
      simpa using h1.trans h2

private theorem perm_pairSums (lv rv : List Nat) :
    (lv.flatMap fun x => rv.map fun y => x + y).Perm
      (lv.mergeSort.flatMap fun x => rv.mergeSort.reverse.map fun y => x + y) := by
  have h1 : (lv.flatMap fun x => rv.map fun y => x + y).Perm
      (lv.mergeSort.flatMap fun x => rv.map fun y => x + y) :=
    perm_flatMap _ (List.mergeSort_perm lv (fun a b : Nat => a ≤ b)).symm
  have h2 : (lv.mergeSort.flatMap fun x => rv.map fun y => x + y).Perm
      (lv.mergeSort.flatMap fun x => rv.mergeSort.map fun y => x + y) := by
    apply perm_flatMap_fun
    intro x hx
    exact ((List.mergeSort_perm rv (fun a b : Nat => a ≤ b)).symm).map (fun y => x + y)
  have h3 : (lv.mergeSort.flatMap fun x => rv.mergeSort.map fun y => x + y).Perm
      (lv.mergeSort.flatMap fun x => rv.mergeSort.reverse.map fun y => x + y) := by
    apply perm_flatMap_fun
    intro x hx
    exact (perm_reverse rv.mergeSort).map (fun y => x + y)
  exact (h1.trans h2).trans h3

private theorem filter_flatMap_map_eq (xs : List Nat) (ys : List Nat) (f : Nat → Nat)
    (p : Nat → Bool) :
    ((xs.flatMap fun x => (ys.map fun y => f x + y)).filter p).length =
      (xs.flatMap fun x => (ys.map fun y => f x + y).filter p).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [List.flatMap_cons, List.filter_append, ih]

private theorem filter_map_add_length (ys : List Nat) (x t : Nat) :
    ((ys.map fun y => x + y).filter fun s : Nat => s ≤ t).length =
      (ys.filter fun y : Nat => x + y ≤ t).length := by
  induction ys with
  | nil => rfl
  | cons y ys ih =>
      by_cases h : x + y ≤ t <;> simp [List.filter_cons, h, ih]

private theorem filter_flatMap_map_add_length (xs ys : List Nat) (t : Nat) :
    ((xs.flatMap fun x => ys.map fun y => x + y).filter fun s : Nat => s ≤ t).length =
      (xs.flatMap fun x => ys.filter fun y : Nat => x + y ≤ t).length := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp [List.flatMap_cons, List.filter_append, ih, filter_map_add_length]

private theorem countLE_eq_ref_count (lv rv : List Nat) (t : Nat) :
    numLE t lv.mergeSort rv.mergeSort.reverse =
      ((lv.flatMap fun x => rv.map fun y => x + y).filter fun s : Nat => s ≤ t).length := by
  unfold numLE
  have hperm := perm_pairSums lv rv
  have hc := List.Perm.countP_eq (fun s : Nat => s ≤ t) hperm
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter] at hc
  have hf := filter_flatMap_map_add_length lv.mergeSort rv.mergeSort.reverse t
  rw [hf] at hc
  exact hc.symm

private theorem map_flatMap {α β γ : Type} (xs : List α) (f : α → β) (g : β → List γ) :
    (xs.map f).flatMap g = xs.flatMap (fun x => g (f x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.flatMap_cons, ih]

private theorem length_flatMap_map (xs ys : List Nat) :
    (xs.flatMap fun x => ys.map fun y => x + y).length = xs.length * ys.length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [List.flatMap_cons, ih, Nat.succ_mul]
      omega

private theorem sums_le_510 (left right : Column) :
    ∀ s ∈ (left.filterMap id).flatMap
        (fun x : UInt8 => (right.filterMap id).map fun y : UInt8 => x.toNat + y.toNat), s ≤ 510 := by
  intro s hs
  simp only [List.mem_flatMap, List.mem_map] at hs
  rcases hs with ⟨x, hx, y, hy, rfl⟩
  have hx255 : x.toNat ≤ 255 := by
    have h := UInt8.toNat_lt x
    omega
  have hy255 : y.toNat ≤ 255 := by
    have h := UInt8.toNat_lt y
    omega
  omega

private theorem all_le_510_filter (sums : List Nat) (h : ∀ s ∈ sums, s ≤ 510) :
    sums.filter (fun s : Nat => s ≤ 510) = sums := by
  apply List.filter_eq_self.mpr
  intro s hs
  exact decide_eq_true (h s hs)

theorem sound : TinylambOpt.ResearchGoals.PairRank.Goal := by
  intro left right k
  let lv := (left.filterMap id).map UInt8.toNat
  let rv := (right.filterMap id).map UInt8.toNat
  let xs := lv.mergeSort
  let ys := rv.mergeSort.reverse
  let sums := (left.filterMap id).flatMap
    (fun x : UInt8 => (right.filterMap id).map fun y : UInt8 => x.toNat + y.toNat)
  let count := fun t => numLE t xs ys
  have hxsAsc : Asc xs := sorted_mergeSort lv
  have hysDesc : Desc ys := sorted_mergeSort_reverse rv
  have hsums_eq : lv.flatMap (fun x => rv.map fun y => x + y) = sums := by
    simp only [lv, rv, sums, List.map_map]
    rw [map_flatMap]
    induction (left.filterMap id) with
    | nil => rfl
    | cons x xxs ih =>
        simp only [List.flatMap_cons]
        rw [ih]
        simp [Function.comp_apply]
  have hcount : ∀ t, count t = (sums.filter fun s : Nat => s ≤ t).length := by
    intro t
    have h := countLE_eq_ref_count lv rv t
    simpa [count, xs, ys, hsums_eq] using h
  have hpermSums : (xs.flatMap fun x => ys.map fun y => x + y).Perm sums := by
    have hp := (perm_pairSums lv rv).symm
    simpa [xs, ys, hsums_eq] using hp
  have hxl : xs.length = (left.filterMap id).length := by simp [xs, lv]
  have hyl : ys.length = (right.filterMap id).length := by simp [ys, rv]
  have hlen_sums : sums.length = xs.length * ys.length := by
    have hp := hpermSums.length_eq
    rw [length_flatMap_map] at hp
    exact hp.symm
  by_cases hkBig : k ≥ xs.length * ys.length
  · have hklen : sums.mergeSort.length ≤ k := by simpa [hlen_sums] using hkBig
    have hnone : sums.mergeSort[k]? = none := List.getElem?_eq_none_iff.mpr hklen
    have hkBig' : k ≥ (left.filterMap id).length * (right.filterMap id).length := by
      simpa [hxl, hyl] using hkBig
    simp [selectSum, reference, lv, rv, xs, ys, sums, hkBig', hnone]
  · have hkSmall : k < xs.length * ys.length := by omega
    have hklen : k < sums.mergeSort.length := by simpa [hlen_sums] using hkSmall
    have hmono : ∀ a b : Nat, a ≤ b → count a ≤ count b := by
      intro a b hab
      rw [hcount a, hcount b]
      exact filter_length_mono (fun s : Nat => s ≤ a) (fun s : Nat => s ≤ b) sums
        (by intro s hs; exact decide_eq_true (by
          have : s ≤ a := of_decide_eq_true hs
          omega))
    have h510 : sums.filter (fun s : Nat => s ≤ 510) = sums :=
      all_le_510_filter sums (sums_le_510 left right)
    have hcount510 : count 510 = xs.length * ys.length := by
      rw [hcount, h510, hlen_sums]
    have hansSearch : IsAnswer count k (search count k 9 0 510) := by
      apply search_is_answer count hmono k 9 0 510
      · rw [hcount510]
        exact hkSmall
      · intro u hu
        omega
      · omega
      · native_decide
    have hpermMerge : sums.mergeSort.Perm sums := List.mergeSort_perm sums (fun a b : Nat => a ≤ b)
    have hcountL : ∀ t, (sums.mergeSort.filter fun s : Nat => s ≤ t).length = count t := by
      intro t
      have hp := List.Perm.countP_eq (fun s : Nat => s ≤ t) hpermMerge
      rw [List.countP_eq_length_filter, List.countP_eq_length_filter] at hp
      exact hp.trans (hcount t).symm
    have hansKth : IsAnswer count k sums.mergeSort[k] := by
      have h := kth_is_answer sums.mergeSort (sorted_mergeSort sums) k hklen
      unfold IsAnswer at h ⊢
      constructor
      · rw [← hcountL]
        exact h.1
      · intro u hu
        rw [← hcountL]
        exact h.2 u hu
    have hsearch : search count k 9 0 510 = sums.mergeSort[k] :=
      isAnswer_unique count k _ _ hansSearch hansKth
    have hkthOpt : sums.mergeSort[k]? = some sums.mergeSort[k] :=
      List.getElem?_eq_some_iff.mpr ⟨hklen, rfl⟩
    have hkSmall' : k < (left.filterMap id).length * (right.filterMap id).length := by
      simpa [hxl, hyl] using hkSmall
    have hfun :
        (fun t => countLE t ((left.filterMap id).length + (right.filterMap id).length)
          xs ys ((right.filterMap id).length)) = count := by
      funext t
      rw [show (left.filterMap id).length = xs.length by exact hxl.symm]
      rw [show (right.filterMap id).length = ys.length by exact hyl.symm]
      exact countLE_eq_numLE t xs ys hxsAsc hysDesc
    have hsearch' :
        search (fun t => countLE t ((left.filterMap id).length + (right.filterMap id).length)
          lv.mergeSort rv.mergeSort.reverse (right.filterMap id).length) k 9 0 510 =
            sums.mergeSort[k] := by
      change search (fun t => countLE t ((left.filterMap id).length + (right.filterMap id).length)
        xs ys ((right.filterMap id).length)) k 9 0 510 = sums.mergeSort[k]
      rw [hfun]
      exact hsearch
    simp [selectSum, reference, lv, rv, sums, hkSmall', hsearch', hkthOpt]

end TinylambOpt.ResearchProofs.PairRank
