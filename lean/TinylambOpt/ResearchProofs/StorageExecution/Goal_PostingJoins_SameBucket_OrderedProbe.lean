import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_OrderedProbe

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, List.getElem?_eq_getElem hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some]
    simp [List.getD, List.getElem?_eq_getElem hi, List.getElem_toArray ha]
  · have ha : xs.toArray.size ≤ i := by simpa using hi
    rw [Array.getElem?_eq_none ha]
    simp [List.getD, List.getElem?_eq_none (by omega : xs.length ≤ i)]

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

/-- Binary search with fuel measured by a halving budget `2^fuel`. -/
private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      simp at hf
      have heq : lo = hi := by omega
      subst hi
      simp only [searchBound]
      exact ⟨⟨hbelow, fun j hj hjlen => habove j (by omega) hjlen⟩,
        Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        exact ⟨⟨hbelow, fun j hj hjlen => habove j (by omega) hjlen⟩,
          trivial, trivial⟩
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := Nat.lt_of_lt_of_le hmh hhi
        have hmidlen : mid < xs.toList.length := by simpa using hmid
        have hval : xs[mid]?.getD 0 = xs[mid] := by
          rw [Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hlistval : xs.toList.getD mid 0 = xs[mid] := by
          rw [← getD_toArray xs.toList mid, Array.getElem?_eq_getElem hmid, Option.getD_some]
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        change Cut xs.toList v
            (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
              else searchBound xs v fuel lo mid) ∧
          lo ≤ (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
            else searchBound xs v fuel lo mid) ∧
          (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
            else searchBound xs v fuel lo mid) ≤ hi
        rw [hval]
        by_cases hv : xs[mid] < v
        · rw [if_pos hv]
          have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              exact hv
            · have hjm : j < mid := by omega
              have hjlen : j < xs.toList.length := by
                simpa using (by omega : j < xs.size)
              have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                rw [getD_eq_getElem hjlen, getD_eq_getElem hmidlen]
                exact hs.rel_getElem_of_lt hjlen hmidlen hjm
              rw [hlistval] at hrel
              by_cases hjlo : j < lo
              · exact hbelow j hjlo
              · omega
          have hf2 : hi - lo < 2 * 2 ^ fuel := by
            simpa [Nat.pow_succ, Nat.mul_comm] using hf
          have hf' : hi - (mid + 1) < 2 ^ fuel := by omega
          clear hf
          have hres := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, by omega, hub⟩
        · rw [if_neg hv]
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              omega
            · have hmj : mid < j := by omega
              have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                rw [getD_eq_getElem hmidlen, getD_eq_getElem hjlen]
                exact hs.rel_getElem_of_lt hmidlen hjlen hmj
              rw [hlistval] at hrel
              by_cases hjhi : hi ≤ j
              · exact habove j hjhi hjlen
              · omega
          have hf2 : hi - lo < 2 * 2 ^ fuel := by
            simpa [Nat.pow_succ, Nat.mul_comm] using hf
          have hf' : mid - lo < 2 ^ fuel := by omega
          clear hf
          have hres := ih lo mid hml (by omega) hf' hbelow habove'
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, hlb, by omega⟩

private theorem fuel_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : (n == 0) = true
  · have hn : n = 0 := by simpa using h
    subst n
    simp
  · have hn : n ≠ 0 := by simpa using h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hn).mp (Nat.lt_succ_self _)
    rw [show n.log2 + 1 + 1 = (n.log2 + 1).succ by omega, Nat.pow_succ]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := fuel_bound ys.length
    simpa using this
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by
      intro j hj hjlen
      have hj' : j < ys.length := by simpa using hjlen
      omega)
    (by simpa using hs)
  simpa [getD_toArray] using h

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).2

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).1.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).1.2 i hj hi

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hba : lower ys b < lower ys a := h
  have hb_lt := lower_lt_of_lt hs a hba
  have hb_len : lower ys b < ys.length :=
    Nat.lt_of_lt_of_le hba (lower_le_length hs a)
  have hb_ge := lower_le_of_ge hs b (Nat.le_refl _) hb_len
  omega

private theorem filter_prefix (p : Nat → Bool) (xs : List Nat) (i : Nat)
    (hi : i < (xs.filter p).length) :
    ∃ j, ∃ h : j < xs.length, i ≤ j ∧ p (xs[j]'h) = true := by
  induction xs generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      by_cases hp : p x = true
      · simp [hp] at hi
        cases i with
        | zero => exact ⟨0, Nat.zero_lt_succ _, Nat.le_refl _, hp⟩
        | succ i =>
            have hi' : i < (xs.filter p).length := by omega
            obtain ⟨j, hj, hij, hpj⟩ := ih i hi'
            exact ⟨j + 1, Nat.succ_lt_succ hj, by omega, by simpa using hpj⟩
      · simp [hp] at hi
        obtain ⟨j, hj, hij, hpj⟩ := ih i hi
        exact ⟨j + 1, Nat.succ_lt_succ hj, by omega, by simpa using hpj⟩

private theorem filter_length_ge_succ_of_sorted_lt {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {j : Nat}
    (hj : j < ys.length) (hjv : ys[j]'hj < v) :
    j + 1 ≤ (ys.filter (fun x => decide (x < v))).length := by
  induction ys generalizing j with
  | nil => simp at hj
  | cons y ys ih =>
      cases j with
      | zero =>
          simp at hjv ⊢
          simp [hjv]
      | succ j =>
          have hs' := hs.tail
          have hjs : j < ys.length := by simpa using hj
          have htailv : ys[j]'hjs < v := by simpa using hjv
          have hres := ih hs' hjs htailv
          by_cases hy : y < v
          · simp [hy] at hres ⊢
            omega
          · have hmem : ys[j]'hjs ∈ ys := List.getElem_mem _
            have hy_le : y ≤ ys[j]'hjs := List.rel_of_pairwise_cons hs hmem
            omega

private theorem cut_unique {xs : List Nat} {v i j : Nat}
    (hilen : i ≤ xs.length) (hjlen : j ≤ xs.length)
    (hi : Cut xs v i) (hj : Cut xs v j) : i = j := by
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro hnot
    have hji : j < i := by omega
    have hjxs : j < xs.length := Nat.lt_of_lt_of_le hji hilen
    have hlt : xs.getD j 0 < v := hi.1 j hji
    have hge : v ≤ xs.getD j 0 := hj.2 j (Nat.le_refl _) hjxs
    omega
  · apply Nat.le_of_not_gt
    intro hnot
    have hij : i < j := by omega
    have hixs : i < xs.length := Nat.lt_of_lt_of_le hij hjlen
    have hlt : xs.getD i 0 < v := hj.1 i hij
    have hge : v ≤ xs.getD i 0 := hi.2 i (Nat.le_refl _) hixs
    omega

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  let p : Nat → Bool := fun x => decide (x < v)
  have hcut : Cut ys v (ys.filter p).length := by
    constructor
    · intro j hj
      obtain ⟨l, hl, hjl, hp⟩ := filter_prefix p ys j hj
      have hjys : j < ys.length := Nat.lt_of_le_of_lt hjl hl
      have hrel : ys.getD j 0 ≤ ys.getD l 0 := by
        by_cases hjl_eq : j = l
        · subst l
          exact Nat.le_refl _
        · have hjlt : j < l := by omega
          rw [getD_eq_getElem hjys, getD_eq_getElem hl]
          exact hs.rel_getElem_of_lt hjys hl hjlt
      have hval : ys[l]'hl < v := by simpa [p] using hp
      rw [getD_eq_getElem hl] at hrel
      omega
    · intro j hj hjl
      by_cases hvle : v ≤ ys.getD j 0
      · exact hvle
      · exfalso
        have hylt : ys.getD j 0 < v := by omega
        have hjv : ys[j]'hjl < v := by
          rw [← getD_eq_getElem hjl]
          exact hylt
        have hjfilter := filter_length_ge_succ_of_sorted_lt hs v hjl hjv
        change (ys.filter (fun x => decide (x < v))).length ≤ j at hj
        omega
  exact cut_unique (lower_le_length hs v) (List.length_filter_le p ys) (lower_cut hs v).1 hcut

private theorem drop_take_eq_take_drop {α : Type} (xs : List α) (i j : Nat) (hij : i ≤ j) :
    (xs.drop i).take (j - i) = (xs.take j).drop i := by
  induction xs generalizing i j with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero => simp
      | succ i =>
          cases j with
          | zero => omega
          | succ j =>
              simp only [List.drop_succ_cons, List.take_succ_cons, Nat.succ_sub_succ]
              exact ih i j (by omega)

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.filter (fun x => decide (x < b)) =
      ys.filter (fun x => decide (x < a)) ++
        ys.filter (fun x => decide (a ≤ x && x < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih hs']
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnil : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hax, hnil, ih hs']
        · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          have hnil_ab : xs.filter (fun y => decide (a ≤ y && y < b)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq, Bool.and_eq_true]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnil_a, ih hs']

private theorem take_filter_prefix {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    ys.take (ys.filter (fun x => decide (x < v))).length =
      ys.filter (fun x => decide (x < v)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hx : x < v
      · simp [hx, ih hs']
      · have hnil : xs.filter (fun y => decide (y < v)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hx, hnil]

private theorem slice_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    slice ys a b = ys.filter (fun y => decide (a ≤ y && y < b)) := by
  unfold slice
  by_cases hab : a ≤ b
  · rw [drop_take_eq_take_drop ys (lower ys a) (lower ys b) (lower_mono hs hab),
      lower_eq_filter_lt_length hs a, lower_eq_filter_lt_length hs b,
      take_filter_prefix hs b, filter_lt_prefix_filter_lt hs hab, List.drop_left]
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero]
    symm
    apply List.filter_eq_nil_iff.mpr
    intro y hy
    simp only [decide_eq_true_eq, Bool.and_eq_true]
    omega

private abbrev opt (a b : Nat) : Ranges :=
  if a < b then [(a, b)] else []

private theorem div_eq_iff_in_bucket {k w y : Nat} (hw : 0 < w) :
    y / w = k ↔ k * w ≤ y ∧ y < (k + 1) * w := by
  constructor
  · intro h
    constructor
    · rw [← h]
      exact Nat.div_mul_le_self y w
    · rw [← h]
      exact (Nat.div_lt_iff_lt_mul hw).mp (Nat.lt_succ_self (y / w))
  · rintro ⟨hlo, hhi⟩
    apply Nat.le_antisymm
    · apply Nat.le_of_not_gt
      intro hk
      have hk' : k + 1 ≤ y / w := by omega
      have hmul : (k + 1) * w ≤ y := (Nat.le_div_iff_mul_le hw).mp hk'
      omega
    · apply Nat.le_of_not_gt
      intro hk
      have hk' : y / w < k := by omega
      have hlt : y < k * w := (Nat.div_lt_iff_lt_mul hw).mp hk'
      omega

theorem sound_OrderedProbe : SameBucket.OrderedProbe.Goal := by
  intro u t x ys hv
  have hs := hv.1
  have hyslt := hv.2
  unfold seek
  dsimp [SameBucket.search]
  let w := t + 1
  let k := x / w
  let a := k * w
  have hw : 0 < w := by dsimp [w]; omega
  change (opt a (min u (a + w))).flatMap (fun (a', b) => slice ys a' b) =
    ys.filter (fun y => k == y / w)
  by_cases hspan : a < min u (a + w)
  · simp [opt, hspan]
    rw [slice_eq_filter hs]
    apply List.filter_congr
    intro y hy
    have hyu := hyslt y hy
    have hiff : (a ≤ y ∧ y < a + w) ↔ k = y / w := by
      constructor
      · intro h
        symm
        apply (div_eq_iff_in_bucket (k := k) (w := w) (y := y) hw).mpr
        constructor
        · simpa [a] using h.1
        · simpa [Nat.succ_mul, a] using h.2
      · intro h
        have hb := (div_eq_iff_in_bucket (k := k) (w := w) (y := y) hw).mp h.symm
        constructor
        · simpa [a] using hb.1
        · simpa [Nat.succ_mul, a] using hb.2
    by_cases hmem : a ≤ y ∧ y < a + w
    · have hk_y : k = y / w := hiff.mp hmem
      have hyaw : y < a + w := hmem.2
      have hyub : y < min u (a + w) := (Nat.lt_min).2 ⟨hyu, hyaw⟩
      simp [hmem.1, hyub, hk_y]
    · have hk_y : k ≠ y / w := by
        intro h
        exact hmem (hiff.mpr h)
      by_cases hay : a ≤ y
      · have hnotaw : ¬ y < a + w := by
          intro hyaw'
          exact hmem ⟨hay, hyaw'⟩
        have hnotub : ¬ y < min u (a + w) := by
          intro hyub
          exact hnotaw ((Nat.lt_min).1 hyub).2
        simp [hay, hnotub, hk_y]
      · simp [hay, hk_y]
  · have hopt : opt a (min u (a + w)) = [] := by simp [opt, hspan]
    rw [hopt]
    simp
    intro y hy
    have hyu := hyslt y hy
    have hk_y : k ≠ y / w := by
      intro h
      have hb := (div_eq_iff_in_bucket (k := k) (w := w) (y := y) hw).mp h.symm
      have hay : a ≤ y := by simpa [a] using hb.1
      have hyaw : y < a + w := by simpa [Nat.succ_mul, a] using hb.2
      have hyub : y < min u (a + w) := (Nat.lt_min).2 ⟨hyu, hyaw⟩
      omega
    simp [hk_y]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_OrderedProbe
