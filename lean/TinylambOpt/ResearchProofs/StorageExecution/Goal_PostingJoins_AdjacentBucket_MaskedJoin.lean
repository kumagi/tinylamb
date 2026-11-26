import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `AdjacentBucket.MaskedJoin` — A point-delete bitmap on the right can subtract removed
  multiplicities from the offset count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_MaskedJoin

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket

/-! Self-contained certificate for `AdjacentBucket.MaskedJoin`.

The binary lower bound on the sorted right key column cuts each of the two
neighbouring buckets exactly; deleting right positions by index subtracts
exactly the matching deleted positions. -/

private theorem optional_eq (a b : Nat) : optional a b = if a < b then [(a, b)] else [] := rfl

private theorem optional_some {a b : Nat} (h : a < b) : optional a b = [(a, b)] := by
  simp [optional_eq, h]

private theorem optional_none {a b : Nat} (h : ¬ a < b) : optional a b = [] := by
  simp [optional_eq, h]

/-- ## Binary lower bound -/

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some, List.getElem_toArray ha]
    simp [List.getD, hi]
  · have ha : xs.toArray.size ≤ i := by simpa using hi
    rw [Array.getElem?_eq_none ha]
    simp [List.getD, hi]

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
      rw [Nat.pow_succ]
      omega
    rw [hpow]
    omega

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have heq : lo = hi := by
        have h2 : (2 : Nat) ^ 0 = 1 := by simp
        omega
      subst hi
      simp only [searchBound]
      exact ⟨⟨hbelow, habove⟩, Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        exact ⟨⟨hbelow, habove⟩, trivial, trivial⟩
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
          rw [Nat.pow_succ]
          omega
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
                  rw [getD_eq_getElem hjlen, getD_eq_getElem hmidlen]
                  exact hs.rel_getElem_of_lt hjlen hmidlen hjm
                rw [hlistval] at hrel
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, Nat.le_trans (by omega : lo ≤ mid + 1) hlb, hub⟩
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
                  rw [hlistval, getD_eq_getElem hjlen]
                  exact hs.rel_getElem_of_lt hmidlen hjlen hmj
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih lo mid hml (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, Nat.le_trans hub (Nat.le_of_lt hmh)⟩

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_pow_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by
      intro j hj hjlen
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

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  have hle : lower ys v ≤ ys.length := lower_le_length hs v
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) = ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [List.getElem_take, decide_eq_true_eq]
    have hi' : i < lower ys v := by omega
    have hiys : i < ys.length := by omega
    have : ys.getD i 0 < v := lower_lt_of_lt hs v hi'
    rwa [getD_eq_getElem hiys] at this
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [List.getElem_drop, decide_eq_true_eq]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := hi
      rw [List.length_drop] at hlen
      omega
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) hiys
    rw [getD_eq_getElem hiys] at hge
    by_cases hxlt : ys[lower ys v + i] < v
    · omega
    · simp [hxlt]
  have hsplit := (List.take_append_drop (lower ys v) ys).symm
  calc
    lower ys v = (ys.take (lower ys v)).length := by
      rw [List.length_take, Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [← hsplit]

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
            intro hay
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

/-- ## Interval contribution of one emitted range -/

private theorem lower_sub_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs, lower_eq_filter_lt_length hs]
  rw [filter_lt_prefix_filter_lt hs hab]
  simp only [List.length_append]
  omega

private theorem lower_min {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (hv : ∀ y ∈ ys, y < u) (v : Nat) :
    lower ys (min u v) = lower ys v := by
  rw [lower_eq_filter_lt_length hs, lower_eq_filter_lt_length hs]
  apply congrArg List.length
  apply List.filter_congr
  intro y hy
  by_cases hyv : y < v
  · have hymin : y < min u v := Nat.lt_min.mpr ⟨hv y hy, hyv⟩
    simp [hyv, hymin]
  · have hymin : ¬ y < min u v := by
      intro h
      exact hyv (Nat.lt_min.mp h).2
    simp [hyv, hymin]

private theorem optional_lower_sum {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (hv : ∀ y ∈ ys, y < u) {p v : Nat} (hpv : p ≤ v) :
    ((optional p (min u v)).map (fun r => lower ys r.2 - lower ys r.1)).sum =
      (ys.filter (fun y => decide (p ≤ y && y < v))).length := by
  by_cases h : p < min u v
  · rw [optional_some h]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
    rw [lower_min hs hv v, lower_sub_eq_filter hs hpv]
  · rw [optional_none h]
    have hnil : ys.filter (fun y => decide (p ≤ y && y < v)) = [] := by
      rw [List.filter_eq_nil_iff]
      intro y hy hpred
      simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
      have hyu : y < u := hv y hy
      exact h (Nat.lt_min.mpr ⟨by omega, by omega⟩)
    rw [hnil]
    simp

/-- ## Bucket arithmetic -/

private theorem int1_iff {w q a y : Nat} (hw : 0 < w) (ha : a = q * w) :
    (a - w ≤ y ∧ y < a) ↔ y / w + 1 = q := by
  by_cases hq : q = 0
  · subst q
    simp [ha]
  · have hqpos : 0 < q := by omega
    have hsub : a - w = (q - 1) * w := by
      rw [ha, Nat.sub_mul]
      simp
    constructor
    · rintro ⟨h1, h2⟩
      have hlo : q - 1 ≤ y / w := (Nat.le_div_iff_mul_le hw).2 (by simpa [hsub] using h1)
      have hhi : y / w < q := (Nat.div_lt_iff_lt_mul hw).2 (by simpa [ha] using h2)
      omega
    · intro h
      have hyq : y / w = q - 1 := by omega
      have hlo' : (q - 1) * w ≤ y := by
        have hmul := Nat.mul_le_mul_right w (by omega : q - 1 ≤ y / w)
        have hdiv : y / w * w ≤ y := Nat.div_mul_le_self y w
        omega
      have hhi' : y < q * w := (Nat.div_lt_iff_lt_mul hw).1 (by omega)
      constructor
      · simpa [hsub] using hlo'
      · simpa [ha] using hhi'

private theorem int2_iff {w q a y : Nat} (hw : 0 < w) (ha : a = q * w) :
    (a + w ≤ y ∧ y < a + 2 * w) ↔ q + 1 = y / w := by
  constructor
  · rintro ⟨h1, h2⟩
    have hlo : q + 1 ≤ y / w := (Nat.le_div_iff_mul_le hw).2 (by simpa [ha, Nat.add_mul] using h1)
    have hhi : y / w < q + 2 := (Nat.div_lt_iff_lt_mul hw).2 (by simpa [ha, Nat.add_mul, Nat.two_mul] using h2)
    omega
  · intro h
    have hlo' : (q + 1) * w ≤ y := by
      have hmul := Nat.mul_le_mul_right w (by omega : q + 1 ≤ y / w)
      have hdiv : y / w * w ≤ y := Nat.div_mul_le_self y w
      omega
    have hhi' : y < (q + 2) * w := (Nat.div_lt_iff_lt_mul hw).1 (by omega)
    constructor
    · simpa [ha, Nat.add_mul] using hlo'
    · simpa [ha, Nat.add_mul, Nat.two_mul] using hhi'

/-- ## Disjoint filters form a permutation -/

private theorem filter_or_append_perm {α : Type} (p q : α → Bool) (xs : List α)
    (hdisj : ∀ x ∈ xs, p x = true → q x = false) :
    (xs.filter p ++ xs.filter q).Perm (xs.filter (fun x => p x || q x)) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have htail : ∀ y ∈ xs, p y = true → q y = false := fun y hy => hdisj y (by simp [hy])
      by_cases hp : p x = true
      · have hq := hdisj x (by simp) hp
        simp [hp, hq, List.Perm.cons x (ih htail)]
      · by_cases hq : q x = true
        · have hswap : (xs.filter p ++ x :: xs.filter q).Perm
              (x :: (xs.filter p ++ xs.filter q)) := by
            simp [List.append_assoc]
          simp [hp, hq]
          exact hswap.trans (List.Perm.cons x (ih htail))
        · simp [hp, hq, ih htail]

private theorem filter_intervals_length (_u t x w q a : Nat) (ys : List Nat)
    (hw : w = t + 1) (hq : q = x / w) (ha : a = q * w) :
    (ys.filter (fun y => decide (a - w ≤ y && y < a))).length +
      (ys.filter (fun y => decide (a + w ≤ y && y < a + 2 * w))).length =
        (ys.filter (search.test t x)).length := by
  have hwpos : 0 < w := by omega
  have hdisj : ∀ y ∈ ys, decide (a - w ≤ y && y < a) = true →
      decide (a + w ≤ y && y < a + 2 * w) = false := by
    intro y hy h1
    simp only [decide_eq_true_eq, Bool.and_eq_true] at h1
    have hn : ¬ (a + w ≤ y ∧ y < a + 2 * w) := by omega
    simp [hn]
  have hperm := filter_or_append_perm
    (fun y => decide (a - w ≤ y && y < a))
    (fun y => decide (a + w ≤ y && y < a + 2 * w)) ys hdisj
  have hfilt : ys.filter (fun y =>
      decide (a - w ≤ y && y < a) || decide (a + w ≤ y && y < a + 2 * w)) =
      ys.filter (search.test t x) := by
    apply List.filter_congr
    intro y hy
    have hi1 : (decide (a - w ≤ y && y < a) = true) ↔ y / w + 1 = q := by
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      exact int1_iff hwpos ha
    have hi2 : (decide (a + w ≤ y && y < a + 2 * w) = true) ↔ q + 1 = y / w := by
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      exact int2_iff hwpos ha
    by_cases h1 : decide (a - w ≤ y && y < a) = true
    · have h2 := hdisj y hy h1
      have heq := hi1.mp h1
      have htest : search.test t x y = true := by
        simp [search, ← hw, ← hq, beq_iff_eq, heq]
      have hp1p : a - w ≤ y ∧ y < a := by
        simpa only [decide_eq_true_eq, Bool.and_eq_true] using h1
      simp [h1, h2, htest]
      omega
    · by_cases h2 : decide (a + w ≤ y && y < a + 2 * w) = true
      · have heq := hi2.mp h2
        have htest : search.test t x y = true := by
          simp [search, ← hw, ← hq, beq_iff_eq, heq]
        have hp2p : a + w ≤ y ∧ y < a + 2 * w := by
          simpa only [decide_eq_true_eq, Bool.and_eq_true] using h2
        simp [h1, h2, htest]
        omega
      · have hne1 : y / w + 1 ≠ q := by intro he; exact h1 (hi1.mpr he)
        have hne2 : q + 1 ≠ y / w := by intro he; exact h2 (hi2.mpr he)
        have htest : search.test t x y = false := by
          simp [search, ← hw, ← hq, beq_iff_eq, hne1, hne2]
        have hn1 : ¬ (a - w ≤ y ∧ y < a) := by
          intro hh
          exact hne1 ((int1_iff hwpos ha).mp hh)
        have hn2 : ¬ (a + w ≤ y ∧ y < a + 2 * w) := by
          intro hh
          exact hne2 ((int2_iff hwpos ha).mp hh)
        simp [h1, h2, htest]
        omega
  rw [← List.length_append, hperm.length_eq, hfilt]

private theorem fan_eq_countP (u t x : Nat) (ys : List Nat) (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  let w := t + 1
  let q := x / w
  let a := q * w
  have hfan : fan search u t x ys =
      (ys.filter (fun y => decide (a - w ≤ y && y < a))).length +
        (ys.filter (fun y => decide (a + w ≤ y && y < a + 2 * w))).length := by
    unfold fan
    dsimp [search]
    rw [List.map_append, List.sum_append]
    change (((optional (a - w) (min u a)).map
        (fun r => lower ys r.2 - lower ys r.1)).sum +
      ((optional (a + w) (min u (a + 2 * w))).map
        (fun r => lower ys r.2 - lower ys r.1)).sum) = _
    rw [optional_lower_sum hv.1 hv.2 (Nat.sub_le a w),
      optional_lower_sum hv.1 hv.2 (by omega : a + w ≤ a + 2 * w)]
  rw [hfan]
  rw [filter_intervals_length u t x w q a ys rfl rfl rfl]
  rw [List.countP_eq_length_filter]

/-- ## Generic counting over zipIdx -/

private theorem zipIdx_eq_map_range (ys : List Nat) (off : Nat) :
    ys.zipIdx off = (List.range ys.length).map (fun i => (ys.getD i 0, off + i)) := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons, ih (off + 1)]
      rw [show (y :: ys).length = ys.length + 1 by simp, List.range_succ_eq_map]
      simp only [List.map_cons, List.getD_cons_zero]
      rw [List.map_map]
      simp [Function.comp_apply, List.getD_cons_succ, Nat.succ_eq_add_one]
      congr 1
      intro i hi
      omega

private theorem countP_range_getD (ys : List Nat) (p : Nat → Bool) :
    (List.range ys.length).countP (fun i => p (ys.getD i 0)) = ys.countP p := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      rw [show (y :: ys).length = ys.length + 1 by simp, List.range_succ_eq_map,
        List.countP_cons, List.countP_map]
      change (List.countP (fun i => p (ys.getD i 0)) (List.range ys.length) +
        (if p y = true then 1 else 0)) = List.countP p (y :: ys)
      rw [ih, List.countP_cons]

private theorem rhs_eq_range (ys ds : List Nat) (p : Nat → Bool) :
    ((ys.zipIdx.filter (fun q => !ds.contains q.2)).map Prod.fst).countP p =
      (List.range ys.length).countP (fun i => p (ys.getD i 0) && !ds.contains i) := by
  rw [zipIdx_eq_map_range ys 0]
  simp [List.filter_map, List.map_map, List.countP_eq_length_filter, List.filter_filter,
    Function.comp_apply, Prod.fst]

private theorem eraseDups_nodup (xs : List Nat) : xs.eraseDups.Nodup := by
  refine (WellFounded.induction (measure List.length).wf (C := fun xs => xs.eraseDups.Nodup) xs ?_)
  intro xs ih
  cases xs with
  | nil => simp
  | cons a xs =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro ha
        have hf := List.mem_eraseDups.mp ha
        simp at hf
      · apply ih
        change (xs.filter (fun b => !b == a)).length < xs.length + 1
        have hle := List.length_filter_le (fun b => !b == a) xs
        omega

private theorem filter_mem_length_eq (xs ys : List Nat) (p : Nat → Bool)
    (hxs : xs.Nodup) (hys : ys.Nodup) :
    (xs.filter (fun i => p i && decide (i ∈ ys))).length =
      (ys.filter (fun i => p i && decide (i ∈ xs))).length := by
  have hm : (xs.filter (fun i => decide (i ∈ ys))).Perm
      (ys.filter (fun i => decide (i ∈ xs))) := by
    apply (List.perm_ext_iff_of_nodup (hxs.filter _) (hys.filter _)).2
    intro i
    simp only [List.mem_filter]
    constructor
    · intro h
      exact ⟨of_decide_eq_true h.2, decide_eq_true h.1⟩
    · intro h
      exact ⟨of_decide_eq_true h.2, decide_eq_true h.1⟩
  have hp := hm.filter p
  simpa only [List.filter_filter] using hp.length_eq

private theorem deleted_count_eq (ys ds : List Nat) (p : Nat → Bool) :
    ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (List.range ys.length).countP (fun i => ds.contains i && p (ys.getD i 0)) := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  have hlhs : ds.eraseDups.filter (fun i => ys[i]?.any p) =
      ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0)) := by
    apply List.filter_congr
    intro i hi
    by_cases hidx : i < ys.length
    · rw [List.getElem?_eq_getElem hidx]
      simp [hidx, getD_eq_getElem hidx]
    · rw [List.getElem?_eq_none (by omega : ys.length ≤ i)]
      simp [hidx]
  rw [hlhs]
  have hlen := filter_mem_length_eq ds.eraseDups (List.range ys.length)
    (fun i => p (ys.getD i 0)) (eraseDups_nodup ds) List.nodup_range
  calc
    (ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0))).length =
        (ds.eraseDups.filter
          (fun i => p (ys.getD i 0) && decide (i ∈ List.range ys.length))).length := by
      apply congrArg List.length
      apply List.filter_congr
      intro i hi
      by_cases hi' : i < ys.length <;> by_cases hpi : p (ys.getD i 0) = true <;> simp [hi', hpi]
    _ = ((List.range ys.length).filter
          (fun i => p (ys.getD i 0) && decide (i ∈ ds.eraseDups))).length := hlen
    _ = ((List.range ys.length).filter
          (fun i => ds.contains i && p (ys.getD i 0))).length := by
      apply congrArg List.length
      apply List.filter_congr
      intro i hi
      rw [List.contains_eq_mem]
      have hmem : decide (i ∈ ds.eraseDups) = decide (i ∈ ds) := by
        simp [List.mem_eraseDups]
      rw [hmem]
      by_cases hpi : p (ys.getD i 0) = true <;> by_cases hdi : i ∈ ds <;> simp [hpi, hdi]

private theorem range_count_split (n : Nat) (ds : List Nat) (f : Nat → Bool) :
    (List.range n).countP f - (List.range n).countP (fun i => ds.contains i && f i) =
      (List.range n).countP (fun i => f i && !ds.contains i) := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter, List.countP_eq_length_filter]
  have hdisj : ∀ i ∈ List.range n, ds.contains i && f i = true →
      (f i && !ds.contains i) = false := by
    intro i hi h
    cases hf : f i <;> cases hd : ds.contains i <;> simp_all
  have hperm := filter_or_append_perm
    (fun i => ds.contains i && f i) (fun i => f i && !ds.contains i) (List.range n) hdisj
  have hfilt : (List.range n).filter f =
      (List.range n).filter (fun i => (ds.contains i && f i) || (f i && !ds.contains i)) := by
    apply List.filter_congr
    intro i hi
    by_cases hd : ds.contains i = true <;> by_cases hfi : f i = true <;> simp [hd, hfi]
  rw [hfilt]
  have hlen := hperm.length_eq
  simp only [List.length_append] at hlen
  omega

theorem sound_MaskedJoin : AdjacentBucket.MaskedJoin.Goal := by
  intro u t x ys ds hv
  rw [fan_eq_countP u t x ys hv]
  let pred := search.test t x
  rw [show ys.countP pred = (List.range ys.length).countP (fun i => pred (ys.getD i 0)) from
    (countP_range_getD ys pred).symm]
  rw [rhs_eq_range ys ds pred]
  rw [deleted_count_eq ys ds pred]
  exact range_count_split ys.length ds (fun i => pred (ys.getD i 0))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_MaskedJoin
