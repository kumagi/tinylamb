import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_RepeatedProbe

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## Interval helpers -/

private theorem optional_eq (a b : Nat) : optional a b = if a < b then [(a, b)] else [] := rfl

private theorem member_optional {a b y : Nat} :
    member (optional a b) y ↔ a ≤ y ∧ y < b := by
  by_cases h : a < b
  · simp [optional_eq, h, member]
  · simp [optional_eq, h, member]
    omega

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) :
    ys.Pairwise (· ≤ ·) := hv.1

private theorem valid_lt {u : Nat} {ys : List Nat} (hv : valid u ys) {y : Nat}
    (hy : y ∈ ys) : y < u := hv.2 y hy

/-! ## Binary lower bound on a sorted column -/

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, List.getElem?_eq_getElem hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some, List.getElem_toArray ha]
    simp [List.getD, List.getElem?_eq_getElem hi]
  · have ha : xs.toArray.size ≤ i := by simpa using hi
    rw [Array.getElem?_eq_none ha]
    simp [List.getD, List.getElem?_eq_none (by omega : xs.length ≤ i)]

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
    have heq : lo = hi := by omega
    subst hi
    exact ⟨⟨hbelow, habove⟩, ⟨by simp [searchBound], by simp [searchBound]⟩⟩
  | succ fuel ih =>
    by_cases hge : lo ≥ hi
    · have heq : lo = hi := by omega
      subst hi
      simp only [searchBound, hge, ↓reduceIte]
      exact ⟨⟨hbelow, habove⟩, ⟨by simp [searchBound], by simp [searchBound]⟩⟩
    · have hlt : lo < hi := by omega
      let mid := (lo + hi) / 2
      have hml : lo ≤ mid := by omega
      have hmh : mid < hi := by omega
      have hmid : mid < xs.size := by omega
      have hmidlen : mid < xs.toList.length := by simpa using hmid
      have hval : xs[mid]?.getD 0 = xs[mid] := by
        rw [Array.getElem?_eq_getElem hmid, Option.getD_some]
      have hlistval : xs.toList.getD mid 0 = xs[mid] := by
        rw [← getD_toArray xs.toList mid, Array.getElem?_eq_getElem hmid, Option.getD_some]
      have hpow : (2 : Nat) ^ (fuel + 1) = 2 * 2 ^ fuel := by
        simp [Nat.pow_succ, Nat.mul_comm]
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
      · rw [ite_eq_left hv]
        have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
          intro j hj
          by_cases heq : j = mid
          · subst j
            rw [hlistval]
            exact hv
          · have hjm : j < mid := by omega
            have hjlen : j < xs.toList.length := by
              simpa using (by omega : j < xs.size)
            by_cases hjl : j < lo
            · exact hbelow j hjl
            · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                have hpair := hs.rel_getElem_of_lt hjlen hmidlen hjm
                rw [← getD_eq_getElem hjlen, ← getD_eq_getElem hmidlen] at hpair
                exact hpair
              rw [hlistval] at hrel
              omega
        have hf' : hi - (mid + 1) < 2 ^ fuel := by
          rw [hpow] at hf
          omega
        have hres := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
        obtain ⟨hc, hlb, hub⟩ := hres
        exact ⟨hc, by omega, hub⟩
      · rw [ite_eq_right hv]
        have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
          intro j hj hjlen
          by_cases heq : j = mid
          · subst j
            rw [hlistval]
            omega
          · have hmj : mid < j := by omega
            by_cases hjh : hi ≤ j
            · exact habove j hjh hjlen
            · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                have hpair := hs.rel_getElem_of_lt hmidlen hjlen hmj
                rw [← getD_eq_getElem hmidlen, ← getD_eq_getElem hjlen] at hpair
                exact hpair
              rw [hlistval] at hrel
              omega
        have hf' : mid - lo < 2 ^ fuel := by
          rw [hpow] at hf
          omega
        have hres := ih lo mid hml (by omega) hf' hbelow habove'
        obtain ⟨hc, hlb, hub⟩ := hres
        exact ⟨hc, hlb, by omega⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst hn
    simp
  · have hne : n ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hpow : (2 : Nat) ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by
      rw [Nat.pow_succ]; omega
    rw [hpow]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_pow_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by intro j hj hjlen
        have hjl : j < ys.length := by simpa using hjlen
        omega)
    (by simpa using hs)
  simpa using h

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
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _)
    (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

private theorem exists_lt_of_filter_nonempty (ys : List Nat) (v : Nat)
    (h : ys.filter (fun x => decide (x < v)) ≠ []) : ∃ y ∈ ys, y < v := by
  induction ys with
  | nil => simp at h
  | cons x xs ih =>
    by_cases hxv : x < v
    · exact ⟨x, by simp, hxv⟩
    · have hne : xs.filter (fun y => decide (y < v)) ≠ [] := by
        intro hz
        apply h
        simp [hxv, hz]
      rcases ih hne with ⟨y, hy, hyv⟩
      exact ⟨y, by simp [hy], hyv⟩

private theorem filter_lt_length_gt_of_getD_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (v : Nat) {n : Nat} (hn : n < ys.length) (hlt : ys.getD n 0 < v) :
    n < (ys.filter (fun x => decide (x < v))).length := by
  induction ys generalizing n with
  | nil => simp at hn
  | cons x xs ih =>
    cases n with
    | zero =>
      simp at hlt
      simp [hlt]
    | succ n =>
      simp only [List.getD_cons_succ] at hlt
      have hn' : n < xs.length := by simpa using hn
      by_cases hxv : x < v
      · have htail := ih hs.tail hn' hlt
        simp [hxv]
        omega
      · have hxle : x ≤ xs.getD n 0 := by
          rw [getD_eq_getElem hn']
          exact List.rel_of_pairwise_cons hs (List.getElem_mem hn')
        omega

private theorem getD_lt_of_filter_lt_length_gt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (v : Nat) {n : Nat} (hn : n < (ys.filter (fun x => decide (x < v))).length) :
    ys.getD n 0 < v := by
  induction ys generalizing n with
  | nil => simp at hn
  | cons x xs ih =>
    cases n with
    | zero =>
      by_cases hxv : x < v
      · simpa using hxv
      · have hnxs : 0 < (xs.filter (fun y => decide (y < v))).length := by
          simpa [hxv] using hn
        have hne : xs.filter (fun y => decide (y < v)) ≠ [] := by
          intro hz
          rw [hz] at hnxs
          simp at hnxs
        rcases exists_lt_of_filter_nonempty xs v hne with ⟨y, hy, hyv⟩
        have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
        omega
    | succ n =>
      by_cases hxv : x < v
      · simp [hxv] at hn
        have hn' : n < (xs.filter (fun y => decide (y < v))).length := by omega
        simpa using ih hs.tail hn'
      · simp [hxv] at hn
        have hn' : n < (xs.filter (fun y => decide (y < v))).length := by omega
        simpa using ih hs.tail hn'

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro h
    have hnlen : (ys.filter (fun x => decide (x < v))).length < ys.length :=
      Nat.lt_of_lt_of_le h (lower_le_length hs v)
    have hlow := lower_lt_of_lt hs v h
    have hgt := filter_lt_length_gt_of_getD_lt hs v hnlen hlow
    omega
  · apply Nat.le_of_not_gt
    intro h
    have hlen : lower ys v < ys.length :=
      Nat.lt_of_lt_of_le h (List.length_filter_le _ _)
    have hlow := getD_lt_of_filter_lt_length_gt hs v h
    have hge := lower_le_of_ge hs v (Nat.le_refl _) hlen
    rw [getD_eq_getElem hlen] at hlow hge
    omega

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
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hxa, hxb, hax, hnil, ih hs']
      · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        have hnil_ab : xs.filter (fun y => decide (a ≤ y && y < b)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [decide_eq_true_eq, Bool.and_eq_true]
          intro hay
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

private theorem lower_sub_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
  rw [filter_lt_prefix_filter_lt hs hab, List.length_append]
  omega

private theorem lower_sub_eq_zero_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hba : b ≤ a) : lower ys b - lower ys a = 0 :=
  Nat.sub_eq_zero_of_le (lower_mono hs hba)

private theorem filter_interval_nil_of_ge (ys : List Nat) {a b : Nat} (hba : b ≤ a) :
    ys.filter (fun y => decide (a ≤ y && y < b)) = [] := by
  rw [List.filter_eq_nil_iff]
  intro y hy
  simp only [decide_eq_true_eq, Bool.and_eq_true]
  intro hay
  omega

/-! ## Ordered intervals in a sorted list -/

private theorem filter_or_append_bounded {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (p q : Nat → Bool) (b : Nat)
    (hp : ∀ y, p y = true → y < b) (hq : ∀ y, q y = true → b ≤ y) :
    ys.filter (fun y => p y || q y) = ys.filter p ++ ys.filter q := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
    have hs' := hs.tail
    by_cases hpx : p x = true
    · have hqx : q x = false := by
        cases hqv : q x
        · rfl
        · exfalso
          have h1 := hp x hpx
          have h2 := hq x (by simpa [hqv])
          omega
      simp [hpx, hqx, ih hs']
    · by_cases hqx : q x = true
      · have hnilp : xs.filter p = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy hpy
          have hyx : x ≤ y := List.rel_of_pairwise_cons hs hy
          have hyb := hp y hpy
          have hxb := hq x hqx
          omega
        simp [hpx, hqx, hnilp, ih hs']
      · simp [hpx, hqx, ih hs']

/-! ## Fan-out of AdjacentBucket is a countP -/

private theorem fan_search_sum_filters (u t x : Nat) {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    let w := t + 1
    let a := x / w * w
    fan search u t x ys =
      (ys.filter (fun y => decide (a - w ≤ y && y < min u a))).length +
      (ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w)))).length := by
  dsimp only
  let w := t + 1
  let a := x / w * w
  change fan search u t x ys =
      (ys.filter (fun y => decide (a - w ≤ y && y < min u a))).length +
      (ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w)))).length
  unfold fan search
  dsimp only
  change ((optional (a - w) (min u a) ++ optional (a + w) (min u (a + 2 * w))).map
      (fun p => lower ys p.2 - lower ys p.1)).sum = _
  rw [List.map_append, List.sum_append]
  simp only [optional_eq]
  by_cases h1 : a - w < min u a
  · rw [if_pos h1]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
    by_cases h2 : a + w < min u (a + 2 * w)
    · rw [if_pos h2]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      have hz1 := lower_sub_eq_filter (ys := ys) (a := a - w) (b := min u a) hs (Nat.le_of_lt h1)
      have hz2 := lower_sub_eq_filter (ys := ys) (a := a + w) (b := min u (a + 2 * w)) hs (Nat.le_of_lt h2)
      rw [hz1, hz2]
    · rw [if_neg h2]
      simp only [List.map_nil, List.sum_nil, Nat.add_zero]
      have hba : min u (a + 2 * w) ≤ a + w := by omega
      have hf2 := filter_interval_nil_of_ge ys hba
      have hz1 := lower_sub_eq_filter (ys := ys) (a := a - w) (b := min u a) hs (Nat.le_of_lt h1)
      rw [hz1, hf2]
      simp
  · rw [if_neg h1]
    simp only [List.map_nil, List.sum_nil, Nat.zero_add]
    by_cases h2 : a + w < min u (a + 2 * w)
    · rw [if_pos h2]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      have hba : min u a ≤ a - w := by omega
      have hf1 := filter_interval_nil_of_ge ys hba
      have hz2 := lower_sub_eq_filter (ys := ys) (a := a + w) (b := min u (a + 2 * w)) hs (Nat.le_of_lt h2)
      rw [hf1, hz2]
      simp
    · rw [if_neg h2]
      simp only [List.map_nil, List.sum_nil, Nat.zero_add]
      have hba1 : min u a ≤ a - w := by omega
      have hba2 : min u (a + 2 * w) ≤ a + w := by omega
      have hf1 := filter_interval_nil_of_ge ys hba1
      have hf2 := filter_interval_nil_of_ge ys hba2
      rw [hf1, hf2]
      simp

private theorem test_eq_intervals (u t x y : Nat) (hyu : y < u) :
    let w := t + 1
    let a := x / w * w
    search.test t x y = true ↔
      (decide (a - w ≤ y && y < min u a) ||
        decide (a + w ≤ y && y < min u (a + 2 * w))) = true := by
  dsimp only
  let w := t + 1
  let a := x / w * w
  let q := x / w
  let b := y / w
  change (q + 1 == b || b + 1 == q) = true ↔
      (decide (a - w ≤ y && y < min u a) ||
        decide (a + w ≤ y && y < min u (a + 2 * w))) = true
  have hwpos : 0 < w := by dsimp [w]; omega
  have hqa : a = q * w := by rfl
  have hmin_a : y < min u a ↔ y < a := by simp [Nat.lt_min, hyu]
  have hmin_aw : y < min u (a + 2 * w) ↔ y < a + 2 * w := by simp [Nat.lt_min, hyu]
  constructor
  · intro ht
    rw [Bool.or_eq_true] at ht
    simp only [beq_iff_eq] at ht
    rcases ht with ht | ht
    · rw [Bool.or_eq_true]
      right
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      constructor
      · have hbw : b * w ≤ y := by dsimp [b]; exact Nat.div_mul_le_self y w
        have hq1 : (q + 1) * w = a + w := by rw [Nat.add_mul, Nat.one_mul, hqa]
        rw [← hq1, ht]
        exact hbw
      · rw [hmin_aw]
        have hblt : b < q + 2 := by omega
        have hylt : y < (q + 2) * w := (Nat.div_lt_iff_lt_mul hwpos).mp (by simpa [b] using hblt)
        have hq2 : (q + 2) * w = a + 2 * w := by rw [Nat.add_mul, hqa]
        rwa [hq2] at hylt
    · rw [Bool.or_eq_true]
      left
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      constructor
      · have hbwmul : b * w ≤ y := by dsimp [b]; exact Nat.div_mul_le_self y w
        have hsub : a - w = b * w := by
          rw [hqa]
          have hqeq : q = b + 1 := by omega
          rw [hqeq, Nat.add_mul, Nat.one_mul, Nat.add_sub_cancel]
        rwa [hsub]
      · rw [hmin_a]
        have hblt : b < q := by omega
        exact (Nat.div_lt_iff_lt_mul hwpos).mp (by simpa [b] using hblt)
  · intro hm
    rw [Bool.or_eq_true] at hm
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hm
    rcases hm with hm | hm
    · have hylt : y < a := by rw [← hmin_a]; exact hm.2
      have hblt : b < q := (Nat.div_lt_iff_lt_mul hwpos).mpr (by simpa [b, hqa] using hylt)
      have hqpos : 0 < q := by
        by_cases hq0 : q = 0
        · rw [hqa, hq0] at hylt
          simp at hylt
        · exact Nat.pos_of_ne_zero hq0
      have hqm1le : q - 1 ≤ b := by
        have hsub : a - w = (q - 1) * w := by
          rw [Nat.sub_mul, Nat.one_mul, ← hqa]
        have hmul : (q - 1) * w ≤ y := by
          rw [← hsub]
          exact hm.1
        have hdiv := (Nat.le_div_iff_mul_le hwpos).mpr hmul
        simpa [b] using hdiv
      clear hqa
      have heq : b + 1 = q := by omega
      simp [Bool.or_eq_true, beq_iff_eq, heq]
    · have hq1mul : (q + 1) * w ≤ y := by
        have hq1 : (q + 1) * w = a + w := by rw [Nat.add_mul, Nat.one_mul, hqa]
        rw [hq1]
        exact hm.1
      have hq1le : q + 1 ≤ b := by
        have hdiv := (Nat.le_div_iff_mul_le hwpos).mpr hq1mul
        simpa [b] using hdiv
      have hylt : y < a + 2 * w := by rw [← hmin_aw]; exact hm.2
      have hblt : b < q + 2 := by
        have hylt' : y < (q + 2) * w := by
          have hq2 : (q + 2) * w = a + 2 * w := by rw [Nat.add_mul, hqa]
          rwa [hq2]
        exact (Nat.div_lt_iff_lt_mul hwpos).mpr (by simpa [b] using hylt')
      clear hqa
      have heq : q + 1 = b := by omega
      simp [Bool.or_eq_true, beq_iff_eq, heq]

private theorem fan_search_countP (u t x : Nat) (ys : List Nat) (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  have hs := valid_pairwise hv
  rw [fan_search_sum_filters u t x hs, List.countP_eq_length_filter]
  let p := fun y => decide (x / (t + 1) * (t + 1) - (t + 1) ≤ y &&
      y < min u (x / (t + 1) * (t + 1)))
  let q := fun y => decide (x / (t + 1) * (t + 1) + (t + 1) ≤ y &&
      y < min u (x / (t + 1) * (t + 1) + 2 * (t + 1)))
  have hp : ∀ y, p y = true → y < x / (t + 1) * (t + 1) := by
    intro y hy
    dsimp [p] at hy
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hy
    have hle := Nat.min_le_left u (x / (t + 1) * (t + 1))
    omega
  have hq : ∀ y, q y = true → x / (t + 1) * (t + 1) ≤ y := by
    intro y hy
    dsimp [q] at hy
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hy
    omega
  have hsplit := filter_or_append_bounded hs p q (x / (t + 1) * (t + 1)) hp hq
  have hfilter : ys.filter (fun y => p y || q y) = ys.filter (search.test t x) := by
    apply List.filter_congr
    intro y hy
    have hiff := test_eq_intervals u t x y (valid_lt hv hy)
    dsimp only [p, q]
    exact Bool.eq_iff_iff.mpr hiff.symm
  change (ys.filter p).length + (ys.filter q).length = (ys.filter (search.test t x)).length
  rw [← hfilter, hsplit, List.length_append]

/-! ## EraseDups multiplicity identity -/

private theorem eraseDups_nodup {α : Type} [DecidableEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  have hmain (n : Nat) : ∀ (l : List α), l.length = n → l.eraseDups.Nodup :=
    Nat.strongRecOn n (fun n ih => by
      intro l hlen
      cases l with
      | nil => simp [List.eraseDups]
      | cons a xs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro ha
          have hm := List.mem_eraseDups.mp ha
          have hf := List.mem_filter.mp hm
          have hbad : (a == a) = false := by simpa using hf.2
          simp at hbad
        · apply ih (List.length (List.filter (fun b => ! b == a) xs))
          · have hle := List.length_filter_le (fun b => ! b == a) xs
            simp only [List.length_cons] at hlen
            omega
          · rfl)
  exact hmain xs.length xs rfl

private theorem sum_map_perm {α : Type} (f : α → Nat) {l₁ l₂ : List α}
    (h : l₁.Perm l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction h with
  | nil => rfl
  | cons a h ih => simp [ih]
  | swap a b l => simp [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_zero_map {α : Type} (xs : List α) (f : α → Nat)
    (h : ∀ x ∈ xs, f x = 0) : (xs.map f).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have hx := h x (by simp)
    have ht := ih (fun y hy => h y (by simp [hy]))
    simp [hx, ht]

private theorem sum_indicator_nodup {α : Type} [DecidableEq α] (xs : List α)
    (hnd : xs.Nodup) (a : α) (v : Nat) (ha : a ∈ xs) :
    (xs.map (fun x => if x = a then v else 0)).sum = v := by
  induction xs with
  | nil => simp at ha
  | cons b bs ih =>
    have hnd' := List.nodup_cons.mp hnd
    by_cases hba : b = a
    · subst b
      have hz := sum_zero_map bs (fun x => if x = a then v else 0)
        (fun x hx => by
          have hxa : x ≠ a := by
            intro heq
            subst x
            exact hnd'.1 hx
          simp [hxa])
      simp [hz]
    · have hmem : a ∈ bs := by
        simp only [List.mem_cons] at ha
        rcases ha with heq | hmem
        · exact (hba heq.symm).elim
        · exact hmem
      simp [hba, ih hnd'.2 hmem]

private theorem count_cons_add {α : Type} [DecidableEq α] [LawfulBEq α]
    (a : α) (xs : List α) (x : α) :
    (a :: xs).count x = xs.count x + if x = a then 1 else 0 := by
  by_cases hxa : x = a
  · subst x
    rw [List.count_cons_self]
    simp
  · rw [List.count_cons_of_ne (Ne.symm hxa)]
    simp [hxa]

private theorem sum_map_eraseDups_count_mul {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (f : α → Nat) :
    (xs.eraseDups.map (fun x => xs.count x * f x)).sum = (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
    by_cases ha : a ∈ xs
    · have hperm_ds : (a :: xs).eraseDups.Perm xs.eraseDups := by
        apply (List.perm_ext_iff_of_nodup (eraseDups_nodup (a :: xs)) (eraseDups_nodup xs)).mpr
        intro x
        constructor
        · intro hx
          have hx' := List.mem_eraseDups.mp hx
          simp only [List.mem_cons] at hx'
          rcases hx' with hxa | hxs
          · subst x
            exact List.mem_eraseDups.mpr ha
          · exact List.mem_eraseDups.mpr hxs
        · intro hx
          exact List.mem_eraseDups.mpr (List.mem_cons_of_mem a (List.mem_eraseDups.mp hx))
      have hstep : ((a :: xs).eraseDups.map (fun x => (a :: xs).count x * f x)).sum =
          (xs.eraseDups.map (fun x => xs.count x * f x)).sum + f a := by
        rw [show ((a :: xs).eraseDups.map (fun x => (a :: xs).count x * f x)).sum =
            (xs.eraseDups.map (fun x => (a :: xs).count x * f x)).sum from
          sum_map_perm _ hperm_ds]
        rw [show xs.eraseDups.map (fun x => (a :: xs).count x * f x) =
            xs.eraseDups.map (fun x => xs.count x * f x + (if x = a then f a else 0)) from by
              apply List.map_congr_left
              intro x hx
              rw [count_cons_add]
              by_cases hxa : x = a <;> simp [hxa, Nat.add_mul]]
        rw [sum_map_add_nat,
          sum_indicator_nodup xs.eraseDups (eraseDups_nodup xs) a (f a)
            (List.mem_eraseDups.mpr ha)]
      rw [hstep, ih]
      simp [List.map_cons, List.sum_cons, Nat.add_comm]
    · have hds : (a :: xs).eraseDups = a :: xs.eraseDups := by
        rw [List.eraseDups_cons]
        have hempty : List.filter (fun b => ! b == a) xs = xs := by
          rw [List.filter_eq_self]
          intro b hb
          have hba : b ≠ a := by intro h; subst b; exact ha hb
          simp [hba]
        rw [hempty]
      rw [hds, List.map_cons, List.sum_cons]
      have hstep : (xs.eraseDups.map (fun x => (a :: xs).count x * f x)).sum =
          (xs.eraseDups.map (fun x => xs.count x * f x)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro x hx
        have hax : a ≠ x := by
          intro h
          subst x
          exact ha (List.mem_eraseDups.mp hx)
        rw [List.count_cons_of_ne hax]
      have hcnt : xs.count a = 0 := (List.count_eq_zero).mpr ha
      rw [hstep, ih]
      rw [List.count_cons_self, hcnt]
      simp

/-! ## Main theorem -/

theorem sound_RepeatedProbe : AdjacentBucket.RepeatedProbe.Goal := by
  intro u t xs ys hv
  have hfan : ∀ x, fan search u t x ys = (ys.filter (search.test t x)).length := by
    intro x
    rw [fan_search_countP u t x ys hv, List.countP_eq_length_filter]
  rw [List.length_flatMap]
  have hkey := sum_map_eraseDups_count_mul xs (fun x => fan search u t x ys)
  have hmap : (xs.map (fun x => fan search u t x ys)).sum =
      (xs.map (fun x => (ys.filter (search.test t x)).length)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact hfan x
  exact hkey.trans hmap

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_RepeatedProbe
