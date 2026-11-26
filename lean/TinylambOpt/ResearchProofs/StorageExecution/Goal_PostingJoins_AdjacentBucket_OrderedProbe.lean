import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_OrderedProbe

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket
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
      have heq : lo = hi := by omega
      subst hi
      simp only [searchBound]
      constructor
      · exact ⟨hbelow, fun j hj hjlen => habove j (by omega) hjlen⟩
      · simp
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        constructor
        · exact ⟨hbelow, fun j hj hjlen => habove j (by omega) hjlen⟩
        · simp
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
              have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                rw [getD_eq_getElem hjlen, getD_eq_getElem hmidlen]
                exact hs.rel_getElem_of_lt hjlen hmidlen hjm
              rw [hlistval] at hrel
              by_cases hjlo : j < lo
              · exact hbelow j hjlo
              · omega
          have hp : 0 < 2 ^ fuel := Nat.pow_pos (by decide : 0 < 2)
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            have hf2 : hi - lo ≤ 2 * 2 ^ fuel - 1 := by
              have hpow := Nat.pow_succ 2 fuel
              omega
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
              have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                rw [getD_eq_getElem hmidlen, getD_eq_getElem hjlen]
                exact hs.rel_getElem_of_lt hmidlen hjlen hmj
              rw [hlistval] at hrel
              by_cases hjhi : hi ≤ j
              · exact habove j hjhi hjlen
              · omega
          have hp : 0 < 2 ^ fuel := Nat.pow_pos (by decide : 0 < 2)
          have hf' : mid - lo < 2 ^ fuel := by
            have hf2 : hi - lo ≤ 2 * 2 ^ fuel - 1 := by
              have hpow := Nat.pow_succ 2 fuel
              omega
            omega
          have hres := ih lo mid hml (by omega) hf' hbelow habove'
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, hlb, by omega⟩

private theorem fuel_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn0 : n = 0 := by simpa using h
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
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by
      intro j hj hjlen
      have hjlen' : j < ys.length := by simpa using hjlen
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

private theorem length_filter_lt_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {v i : Nat}
    (hi : i < ys.length) (hlt : ys[i] < v) :
    i + 1 ≤ (ys.filter (fun x => decide (x < v))).length := by
  induction ys generalizing i with
  | nil => simp at hi
  | cons y ys ih =>
      cases i with
      | zero =>
          have hy : y < v := by simpa using hlt
          simp [hy]
      | succ i =>
          have hi' : i < ys.length := by simpa using hi
          have hlt' : ys[i] < v := by simpa using hlt
          have htail := ih hs.tail hi' hlt'
          by_cases hy : y < v
          · simp [hy]
            omega
          · have hymem : ys[i] ∈ ys := List.getElem_mem hi'
            have hyle : y ≤ ys[i] := List.rel_of_pairwise_cons hs hymem
            exfalso
            omega

private theorem length_filter_lt_le {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {v i : Nat}
    (hi : i < ys.length) (hge : v ≤ ys[i]) :
    (ys.filter (fun x => decide (x < v))).length ≤ i := by
  induction ys generalizing i with
  | nil => simp at hi
  | cons y ys ih =>
      cases i with
      | zero =>
          have hyge : v ≤ y := by simpa using hge
          have hnil : ys.filter (fun x => decide (x < v)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro z hz
            simp only [decide_eq_true_eq]
            have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
            omega
          simp [hyge, hnil]
      | succ i =>
          have hi' : i < ys.length := by simpa using hi
          have hge' : v ≤ ys[i] := by simpa using hge
          have htail := ih hs.tail hi' hge'
          by_cases hy : y < v
          · simp [hy]
            omega
          · simp [hy]
            omega

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro h
    have hnlen : (ys.filter (fun x => decide (x < v))).length < ys.length :=
      Nat.lt_of_lt_of_le h (lower_le_length hs v)
    have hlow := lower_lt_of_lt hs v h
    rw [getD_eq_getElem hnlen] at hlow
    have hcnt := length_filter_lt_ge hs hnlen hlow
    omega
  · apply Nat.le_of_not_gt
    intro h
    have hln : lower ys v < (ys.filter (fun x => decide (x < v))).length := h
    have hlen : lower ys v < ys.length :=
      Nat.lt_of_lt_of_le hln (List.length_filter_le _ _)
    have hge := lower_le_of_ge hs v (Nat.le_refl _) hlen
    rw [getD_eq_getElem hlen] at hge
    have hcnt := length_filter_lt_le hs hlen hge
    omega

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
  TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b

private theorem lt_min_of_lt {a b c : Nat} (hca : c < a) (hcb : c < b) : c < min a b := by
  by_cases hab : a ≤ b
  · rw [Nat.min_eq_left hab]
    exact hca
  · rw [Nat.min_eq_right (by omega : b ≤ a)]
    exact hcb

-- Adjacent buckets: test t x y holds iff bucket(y) = bucket(x) ± 1.
-- The two half-open spans are exactly those two buckets, clipped to [0,u).

private theorem test_iff_intervals {u t x y : Nat} (hyu : y < u) :
    search.test t x y = true ↔
      (x / (t + 1) * (t + 1) - (t + 1) ≤ y ∧
          y < min u (x / (t + 1) * (t + 1))) ∨
      (x / (t + 1) * (t + 1) + (t + 1) ≤ y ∧
          y < min u (x / (t + 1) * (t + 1) + 2 * (t + 1))) := by
  let w := t + 1
  let q := x / w
  let a := q * w
  have hw : 0 < w := by dsimp [w]; omega
  change (q + 1 == y / w || y / w + 1 == q) = true ↔
    (a - w ≤ y ∧ y < min u a) ∨ (a + w ≤ y ∧ y < min u (a + 2 * w))
  simp only [Bool.or_eq_true, beq_iff_eq]
  constructor
  · rintro (hnext | hprev)
    · have hlo : (q + 1) * w ≤ y := by
        have h0 := Nat.div_mul_le_self y w
        rw [← hnext] at h0
        exact h0
      have hlt : y < (q + 2) * w := by
        apply (Nat.div_lt_iff_lt_mul hw).mp
        rw [← hnext]
        omega
      right
      constructor
      · simpa [Nat.succ_mul, a] using hlo
      · have hyup : y < a + 2 * w := by
          have heq : (q + 2) * w = a + 2 * w := by
            dsimp [a]
            simp [Nat.succ_mul, Nat.add_assoc]
          rwa [heq] at hlt
        exact lt_min_of_lt hyu hyup
    · have hqpos : 0 < q := by
        have hqeq : q = y / w + 1 := hprev.symm
        rw [hqeq]
        exact Nat.succ_pos _
      have hydiv : y / w = q - 1 := by
        rw [← hprev]
        omega
      have hsub : a - w = (q - 1) * w := by
        dsimp [a]
        rw [Nat.sub_mul]
        simp
      have hlo : (q - 1) * w ≤ y := by
        rw [← hydiv]
        exact Nat.div_mul_le_self y w
      have hlt : y < q * w := by
        apply (Nat.div_lt_iff_lt_mul hw).mp
        omega
      left
      constructor
      · simpa [hsub] using hlo
      · have hya : y < a := by simpa [a] using hlt
        exact lt_min_of_lt hyu hya
  · intro h
    rcases h with hprev | hnext
    · right
      have hqpos : 0 < q := by
        by_cases hq0 : q = 0
        · have ha0 : a = 0 := by dsimp [a]; simp [hq0]
          have hya : y < a := by
            have hmin := Nat.min_le_right u a
            exact Nat.lt_of_lt_of_le hprev.2 hmin
          omega
        · exact Nat.pos_of_ne_zero hq0
      have hsub : a - w = (q - 1) * w := by
        dsimp [a]
        rw [Nat.sub_mul]
        simp
      have hsucc : (q - 1) + 1 = q := Nat.sub_add_cancel hqpos
      have hya : y < a := by
        have hmin := Nat.min_le_right u a
        exact Nat.lt_of_lt_of_le hprev.2 hmin
      have hdiv : y / w = q - 1 := by
        apply Nat.div_eq_of_lt_le
        · simpa [hsub] using hprev.1
        · simpa [hsucc, a] using hya
      rw [hdiv]
      exact hsucc
    · left
      have hlo : (q + 1) * w ≤ y := by
        simpa [Nat.succ_mul, a] using hnext.1
      have hyup : y < a + 2 * w := by
        have hmin := Nat.min_le_right u (a + 2 * w)
        exact Nat.lt_of_lt_of_le hnext.2 hmin
      have heq : (q + 2) * w = a + 2 * w := by
        dsimp [a]
        simp [Nat.succ_mul, Nat.add_assoc]
      have hhi : y < (q + 2) * w := by
        rwa [← heq] at hyup
      have hdiv : y / w = q + 1 := Nat.div_eq_of_lt_le hlo hhi
      exact hdiv.symm

-- For a sorted list, two disjoint ordered intervals can be filtered in one pass.
private theorem filter_two_ordered {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b c d : Nat} (hbc : b ≤ c) :
    ys.filter (fun y => decide (a ≤ y && y < b)) ++
        ys.filter (fun y => decide (c ≤ y && y < d)) =
      ys.filter (fun y => decide ((a ≤ y && y < b) || (c ≤ y && y < d))) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have hs' := hs.tail
      have hy_le : ∀ z ∈ ys, y ≤ z := fun z hz => List.rel_of_pairwise_cons hs hz
      by_cases hp : (a ≤ y && y < b) = true
      · have hp0 : a ≤ y ∧ y < b := by simpa only [Bool.and_eq_true, decide_eq_true_eq] using hp
        have hq0 : ¬ (c ≤ y ∧ y < d) := by intro h; omega
        have hq : (c ≤ y && y < d) = false := by
          apply eq_false_of_ne_true
          intro hcd
          have hcd0 : c ≤ y ∧ y < d := by
            simpa only [Bool.and_eq_true, decide_eq_true_eq] using hcd
          exact hq0 hcd0
        have hpq : ((a ≤ y && y < b) || (c ≤ y && y < d)) = true := by simp [hp]
        simp [hp, hq, hpq]
        exact ih hs'
      · by_cases hq : (c ≤ y && y < d) = true
        · have hq0 : c ≤ y ∧ y < d := by simpa only [Bool.and_eq_true, decide_eq_true_eq] using hq
          have hnil : ys.filter (fun z => decide (a ≤ z) && decide (z < b)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro z hz hzp
            have hzp0 : a ≤ z ∧ z < b := by simpa only [Bool.and_eq_true, decide_eq_true_eq] using hzp
            have hyz := hy_le z hz
            omega
          have hpq : ((a ≤ y && y < b) || (c ≤ y && y < d)) = true := by simp [hq]
          simp [hp, hq, hpq]
          have hi : ys.filter (fun y => decide (a ≤ y) && decide (y < b)) ++
                ys.filter (fun y => decide (c ≤ y) && decide (y < d)) =
              ys.filter (fun y => decide (a ≤ y) && decide (y < b) ||
                decide (c ≤ y) && decide (y < d)) := by
            simpa [decide_eq_true_eq, Bool.and_eq_true] using ih hs'
          rw [hnil] at hi ⊢
          exact congrArg (fun l : List Nat => y :: l) hi
        · have hpq : ((a ≤ y && y < b) || (c ≤ y && y < d)) = false := by simp [hp, hq]
          simp [hp, hq, hpq]
          exact ih hs'

private theorem flatMap_optional (a b : Nat) (f : Nat × Nat → List Nat) :
    (opt a b).flatMap f = if a < b then f (a, b) else [] := by
  by_cases h : a < b
  · simp [opt, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]
  · simp [opt, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

theorem sound_OrderedProbe : AdjacentBucket.OrderedProbe.Goal := by
  intro u t x ys hv
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  have hyslt : ∀ y ∈ ys, y < u := hv.2
  unfold seek
  dsimp [search]
  let w := t + 1
  let q := x / w
  let a := q * w
  have hw : 0 < w := by dsimp [w]; omega
  change (opt (a - w) (min u a) ++ opt (a + w) (min u (a + 2 * w))).flatMap
      (fun (lo, hi) => slice ys lo hi) = ys.filter (search.test t x)
  have hiff_of_mem : ∀ y ∈ ys,
      search.test t x y = true ↔
        (a - w ≤ y ∧ y < min u a) ∨ (a + w ≤ y ∧ y < min u (a + 2 * w)) := by
    intro y hy
    simpa [w, q, a] using
      (test_iff_intervals (u := u) (t := t) (x := x) (y := y) (hyslt y hy))
  rw [List.flatMap_append, flatMap_optional, flatMap_optional]
  by_cases h1 : a - w < min u a
  · by_cases h2 : a + w < min u (a + 2 * w)
    · rw [if_pos h1, if_pos h2]
      simp only
      rw [slice_eq_filter hs, slice_eq_filter hs]
      have hfilter :
          ys.filter (search.test t x) =
            ys.filter (fun y => decide (a - w ≤ y && y < min u a)) ++
              ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w))) := by
        calc
          ys.filter (search.test t x) =
              ys.filter (fun y => decide ((a - w ≤ y && y < min u a) ||
                (a + w ≤ y && y < min u (a + 2 * w)))) := by
                  apply List.filter_congr
                  intro y hy
                  have hi := hiff_of_mem y hy
                  apply Bool.eq_iff_iff.mpr
                  rw [decide_eq_true_eq]
                  simpa only [Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq] using hi
          _ = ys.filter (fun y => decide (a - w ≤ y && y < min u a)) ++
                ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w))) := by
                  symm
                  apply filter_two_ordered hs
                  omega
      exact hfilter.symm
    · rw [if_pos h1, if_neg h2]
      simp only [List.append_nil]
      rw [slice_eq_filter hs]
      apply List.filter_congr
      intro y hy
      have hi := hiff_of_mem y hy
      have hp2 : ¬ (a + w ≤ y ∧ y < min u (a + 2 * w)) := by
        intro h
        have hle : min u (a + 2 * w) ≤ a + w := Nat.le_of_not_lt h2
        omega
      have hi1 : search.test t x y = true ↔ (a - w ≤ y ∧ y < min u a) := by
        constructor
        · intro ht
          exact (hi.mp ht).resolve_right hp2
        · intro hp
          exact hi.mpr (Or.inl hp)
      apply Bool.eq_iff_iff.mpr
      rw [decide_eq_true_eq]
      simpa only [Bool.and_eq_true, decide_eq_true_eq] using hi1.symm
  · by_cases h2 : a + w < min u (a + 2 * w)
    · rw [if_neg h1, if_pos h2]
      simp only [List.nil_append]
      rw [slice_eq_filter hs]
      apply List.filter_congr
      intro y hy
      have hi := hiff_of_mem y hy
      have hp1 : ¬ (a - w ≤ y ∧ y < min u a) := by
        intro h
        have hle : min u a ≤ a - w := Nat.le_of_not_lt h1
        omega
      have hi2 : search.test t x y = true ↔ (a + w ≤ y ∧ y < min u (a + 2 * w)) := by
        constructor
        · intro ht
          exact (hi.mp ht).resolve_left hp1
        · intro hp
          exact hi.mpr (Or.inr hp)
      apply Bool.eq_iff_iff.mpr
      rw [decide_eq_true_eq]
      simpa only [Bool.and_eq_true, decide_eq_true_eq] using hi2.symm
    · rw [if_neg h1, if_neg h2]
      simp only [List.nil_append, List.append_nil]
      symm
      apply List.filter_eq_nil_iff.mpr
      intro y hy
      have hi := hiff_of_mem y hy
      have hp1 : ¬ (a - w ≤ y ∧ y < min u a) := by
        intro h
        have hle : min u a ≤ a - w := Nat.le_of_not_lt h1
        omega
      have hp2 : ¬ (a + w ≤ y ∧ y < min u (a + 2 * w)) := by
        intro h
        have hle : min u (a + 2 * w) ≤ a + w := Nat.le_of_not_lt h2
        omega
      intro ht
      rcases hi.mp ht with hp | hp
      · exact hp1 hp
      · exact hp2 hp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_OrderedProbe
