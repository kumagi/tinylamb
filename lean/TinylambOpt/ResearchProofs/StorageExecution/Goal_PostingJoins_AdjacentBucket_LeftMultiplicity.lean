import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `AdjacentBucket.LeftMultiplicity` — Charge one left placeholder when zero right keys
  match, including a NULL left key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_LeftMultiplicity

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## Lower bound on a sorted key column -/

private def Cut (ys : List Nat) (v i : Nat) : Prop :=
  i ≤ ys.length ∧
  (∀ j, j < i → ys.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < ys.length → v ≤ ys.getD j 0)

private theorem array_mono {xs : Array Nat} (hs : xs.toList.Pairwise (· ≤ ·))
    {i j : Nat} (hi : i < xs.size) (hj : j < xs.size) (hij : i < j) :
    xs[i]?.getD 0 ≤ xs[j]?.getD 0 := by
  rw [← Array.getElem?_toList (xs := xs) (i := i),
    ← Array.getElem?_toList (xs := xs) (i := j)]
  have hi' : i < xs.toList.length := by simpa using hi
  have hj' : j < xs.toList.length := by simpa using hj
  have hrel : xs.toList[i] ≤ xs.toList[j] := hs.rel_getElem_of_lt hi' hj' hij
  simpa [List.getD, List.getElem?_eq_getElem hi', List.getElem?_eq_getElem hj'] using hrel

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs[j]?.getD 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.size → v ≤ xs[j]?.getD 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have heq : lo = hi := by omega
      subst hi
      simp [searchBound]
      refine ⟨by simpa using hhi, ?_, ?_⟩
      · intro j hj
        simpa [List.getD, Array.getElem?_toList] using hbelow j hj
      · intro j hj hjlen
        simpa [List.getD, Array.getElem?_toList] using habove j hj (by simpa using hjlen)
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp [searchBound]
        refine ⟨by simpa using hhi, ?_, ?_⟩
        · intro j hj
          simpa [List.getD, Array.getElem?_toList] using hbelow j hj
        · intro j hj hjlen
          simpa [List.getD, Array.getElem?_toList] using habove j hj (by simpa using hjlen)
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := by omega
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        by_cases hv : xs[mid]?.getD 0 < v
        · rw [if_pos hv]
          have hbelow' : ∀ j, j < mid + 1 → xs[j]?.getD 0 < v := by
            intro j hj
            by_cases hjmid : j = mid
            · subst j
              exact hv
            · have hjlt : j < mid := by omega
              by_cases hjlo : j < lo
              · exact hbelow j hjlo
              · have hjltSize : j < xs.size := by omega
                have hmono := array_mono hs hjltSize hmid hjlt
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            have hp : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by
              rw [Nat.pow_succ, Nat.mul_comm]
            rw [hp] at hf
            omega
          have hres := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          obtain ⟨hc, hlb, hub⟩ := hres
          refine ⟨hc, ?_, hub⟩
          exact Nat.le_trans hml (Nat.le_trans (Nat.le_succ mid) hlb)
        · rw [if_neg hv]
          have habove' : ∀ j, mid ≤ j → j < xs.size → v ≤ xs[j]?.getD 0 := by
            intro j hj hjlen
            by_cases hjmid : j = mid
            · subst j
              omega
            · have hmj : mid < j := by omega
              by_cases hjhi : hi ≤ j
              · exact habove j hjhi hjlen
              · have hmono := array_mono hs hmid hjlen hmj
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            have hp : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by
              rw [Nat.pow_succ, Nat.mul_comm]
            rw [hp] at hf
            omega
          have hres := ih lo mid hml (by omega) hf' hbelow habove'
          obtain ⟨hc, hlb, hub⟩ := hres
          refine ⟨hc, hlb, ?_⟩
          exact Nat.le_trans hub (Nat.le_of_lt hmh)

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn0 : n = 0 := by simpa using h
    subst n
    simp
  · simp [h]
    have hn : n ≠ 0 := by simpa using h
    have hlt : n < 2 ^ (n.log2 + 1) :=
      (Nat.log2_lt hn).mp (by omega)
    exact Nat.lt_of_lt_of_le hlt
      (Nat.pow_le_pow_right (by decide : 0 < 2) (by omega : n.log2 + 1 ≤ n.log2 + 1 + 1))

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) := by
  unfold lower
  have hsize : ys.toArray.size = ys.length := by simp
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_pow_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by omega) hf
    (by intro j hj; omega)
    (by intro j hj hjlen; omega)
    (by simpa using hs)
  simpa [List.getD, Array.getElem?_toList] using h.1

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).1

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).2.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : lower ys v ≤ i) (hj : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).2.2 i hi hj

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have hla := lower_le_length hs a
  have ha := lower_le_of_ge hs b (Nat.le_refl _) (by omega)
  omega

/-! ## The cut at the filter length -/

private theorem filter_count_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v ((ys.filter (fun y => decide (y < v))).length) := by
  induction ys with
  | nil => simp [Cut]
  | cons a tl ih =>
      have hs' : tl.Pairwise (· ≤ ·) := hs.tail
      have hcut := ih hs'
      by_cases ha : a < v
      · simp only [List.filter_cons, decide_eq_true_eq, ha, ↓reduceIte, List.length_cons]
        have hfl : (tl.filter (fun y => decide (y < v))).length ≤ tl.length :=
          List.length_filter_le _ _
        refine ⟨?_, ?_, ?_⟩
        · simpa [List.length_cons, Nat.add_comm] using (Nat.add_le_add_right hfl 1)
        · intro j hj
          cases j with
          | zero => simpa using ha
          | succ j =>
            have hj' : j < (tl.filter (fun y => decide (y < v))).length := by omega
            simpa using hcut.2.1 j hj'
        · intro j hj hjlen
          cases j with
          | zero => omega
          | succ j =>
            have hj' : j < tl.length := by simpa using hjlen
            have hge : (tl.filter (fun y => decide (y < v))).length ≤ j := by omega
            simpa using hcut.2.2 j hge hj'
      · have hgeav : v ≤ a := by omega
        have hnil : tl.filter (fun y => decide (y < v)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          simp only [decide_eq_true_eq]
          have hay : a ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp only [List.filter_cons, decide_eq_true_eq, ha, ↓reduceIte, hnil, List.length_nil, Nat.zero_add]
        refine ⟨by omega, ?_, ?_⟩
        · intro j hj
          omega
        · intro j hj hjlen
          cases j with
          | zero => simpa using hgeav
          | succ j =>
            have hj' : j < tl.length := by simpa using hjlen
            have hge : (tl.filter (fun y => decide (y < v))).length ≤ j := by
              simpa [hnil]
            simpa using hcut.2.2 j hge hj'

private theorem cut_unique {ys : List Nat} {v i k : Nat}
    (hi : Cut ys v i) (hk : Cut ys v k) : i = k := by
  have hil := hi.1
  have hkl := hk.1
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro hki
    have hl := hi.2.1 k hki
    have hg := hk.2.2 k (Nat.le_refl _) (by omega)
    omega
  · apply Nat.le_of_not_gt
    intro hik
    have hl := hk.2.1 i hik
    have hg := hi.2.2 i (Nat.le_refl _) (by omega)
    omega

private theorem lower_eq_filter_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun y => decide (y < v))).length :=
  cut_unique (lower_cut hs v) (filter_count_cut hs v)

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.filter (fun y => decide (y < b)) =
      ys.filter (fun y => decide (y < a)) ++
        ys.filter (fun y => decide (a ≤ y && y < b)) := by
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

private theorem lower_sub_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (a b : Nat) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_length hs b, lower_eq_filter_length hs a]
  by_cases hab : a ≤ b
  · have hsplit := filter_lt_prefix_filter_lt hs hab
    rw [hsplit, List.length_append]
    omega
  · have hb : b ≤ a := by omega
    have hsub : (ys.filter (fun y => decide (y < b))).length ≤
        (ys.filter (fun y => decide (y < a))).length := by
      have hsname : (ys.filter (fun y => decide (y < b))).length =
          lower ys b := (lower_eq_filter_length hs b).symm
      have hsname2 : (ys.filter (fun y => decide (y < a))).length =
          lower ys a := (lower_eq_filter_length hs a).symm
      rw [hsname, hsname2]
      exact lower_mono hs hb
    rw [Nat.sub_eq_zero_of_le hsub]
    have hnil : ys.filter (fun y => decide (a ≤ y && y < b)) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro y hy
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      intro hay
      omega
    rw [hnil]
    rfl

/-! ## Sum of two disjoint filters -/

private def opt (a b : Nat) : Ranges :=
  TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b

private theorem opt_some {a b : Nat} (h : a < b) : opt a b = [(a, b)] := by
  unfold opt TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional
  simp [h]

private theorem opt_none {a b : Nat} (h : ¬ a < b) : opt a b = [] := by
  unfold opt TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional
  simp [h]

private theorem filter_or_length {α : Type} (p q : α → Bool) (xs : List α)
    (hdisj : ∀ a ∈ xs, p a = true → q a = false) :
    (xs.filter p).length + (xs.filter q).length =
      (xs.filter (fun a => p a || q a)).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have htail : ∀ a ∈ xs, p a = true → q a = false :=
        fun a ha => hdisj a (by simp [ha])
      cases hpx : p x <;> cases hqx : q x <;> simp_all <;> omega

/-! ## AdjacentBucket arithmetic -/

private theorem adjacent_span_arith {w q a : Nat} (ha : a = w * q) :
    (∀ hq1 : 1 ≤ q, a - w = (q - 1) * w) ∧
    a + w = (q + 1) * w ∧
    a + 2 * w = (q + 2) * w := by
  constructor
  · intro hq1
    have hqeq : (q - 1) + 1 = q := Nat.sub_add_cancel hq1
    calc
      a - w = q * w - w := by rw [ha, Nat.mul_comm]
      _ = ((q - 1) + 1) * w - w := by rw [hqeq]
      _ = ((q - 1) * w + w) - w := by rw [Nat.succ_mul]
      _ = (q - 1) * w := Nat.add_sub_cancel _ _
  constructor
  · rw [Nat.succ_mul]
    rw [ha, Nat.mul_comm w q]
  · rw [Nat.add_mul]
    rw [ha, Nat.mul_comm w q]

private theorem adjacent_test_iff {u w q a v y : Nat} (hw : 0 < w) (hq : q = v / w)
    (ha : a = w * q) (hyu : y < u) :
    ((a - w ≤ y ∧ y < min u a) ∨ (a + w ≤ y ∧ y < min u (a + 2 * w))) ↔
      (q + 1 == y / w || y / w + 1 == q) = true := by
  obtain ⟨hsub, haddw, hadd2w⟩ := adjacent_span_arith ha
  constructor
  · intro h
    rcases h with ⟨hlo, hhi⟩ | ⟨hlo, hhi⟩
    · have hya : y < a := Nat.lt_of_lt_of_le hhi (Nat.min_le_right u a)
      have hq1 : 1 ≤ q := by
        by_cases hq0 : q = 0
        · rw [hq0] at ha
          have ha0 : a = 0 := by simpa using ha
          omega
        · omega
      have hdiv : y / w = q - 1 := by
        apply Nat.div_eq_of_lt_le
        · rw [← hsub hq1]
          exact hlo
        · rw [Nat.sub_add_cancel hq1, Nat.mul_comm q w, ← ha]
          exact hya
      have hEq : y / w + 1 = q := by omega
      have hNe : q + 1 ≠ y / w := by omega
      simp [hNe, hEq]
    · have hya : y < a + 2 * w := Nat.lt_of_lt_of_le hhi (Nat.min_le_right u (a + 2 * w))
      have hdiv : y / w = q + 1 := by
        apply Nat.div_eq_of_lt_le
        · rw [← haddw]
          exact hlo
        · have heq : q + 1 + 1 = q + 2 := by omega
          rw [heq, ← hadd2w]
          exact hya
      have hNe : y / w + 1 ≠ q := by omega
      simp [hNe, hdiv]
  · intro ht
    simp only [Bool.or_eq_true, decide_eq_true_eq, beq_iff_eq] at ht
    rcases ht with ht | ht
    · right
      have hge : (q + 1) * w ≤ y := by
        apply (Nat.le_div_iff_mul_le hw).mp
        rw [ht]
        exact Nat.le_refl _
      have hlt : y < (q + 2) * w := by
        apply (Nat.div_lt_iff_lt_mul hw).mp
        rw [← ht]
        omega
      constructor
      · rw [haddw]
        exact hge
      · exact Nat.lt_min.mpr ⟨hyu, by
          rw [hadd2w]
          exact hlt⟩
    · left
      have hq1 : 1 ≤ q := by
        rw [← ht]
        exact Nat.succ_le_succ (Nat.zero_le _)
      have hydiv : y / w = q - 1 := by
        rw [← ht]
        simp
      have hge : (q - 1) * w ≤ y := by
        apply (Nat.le_div_iff_mul_le hw).mp
        rw [hydiv]
        exact Nat.le_refl _
      have hlt : y < q * w := by
        apply (Nat.div_lt_iff_lt_mul hw).mp
        rw [hydiv]
        omega
      constructor
      · rw [hsub hq1]
        exact hge
      · exact Nat.lt_min.mpr ⟨hyu, by
          rw [ha, Nat.mul_comm w q]
          exact hlt⟩

private theorem adjacent_test_bool {u w q a v y : Nat} (hw : 0 < w) (hq : q = v / w)
    (ha : a = w * q) (hyu : y < u) :
    decide (a - w ≤ y && y < min u a) || decide (a + w ≤ y && y < min u (a + 2 * w)) =
      (q + 1 == y / w || y / w + 1 == q) := by
  have hiff := adjacent_test_iff hw hq ha hyu
  by_cases hL : a - w ≤ y ∧ y < min u a
  · have hR : ¬ (a + w ≤ y ∧ y < min u (a + 2 * w)) := by
      intro hR
      have hy1 : y < a := Nat.lt_of_lt_of_le hL.2 (Nat.min_le_right u a)
      have hy2 : a + w ≤ y := hR.1
      omega
    have hT : (q + 1 == y / w || y / w + 1 == q) = true := hiff.mp (Or.inl hL)
    simp [hL, hR, hT]
  · by_cases hR : a + w ≤ y ∧ y < min u (a + 2 * w)
    · have hT : (q + 1 == y / w || y / w + 1 == q) = true := hiff.mp (Or.inr hR)
      simp [hL, hR, hT]
    · have hTf : (q + 1 == y / w || y / w + 1 == q) = false := by
        cases hT : (q + 1 == y / w || y / w + 1 == q)
        · rfl
        · exfalso
          have hd := hiff.mpr hT
          rcases hd with hd | hd
          · exact hL hd
          · exact hR hd
      simp [hL, hR, hTf]

private theorem adjacent_pred_disjoint {u w a y : Nat} :
    decide (a - w ≤ y && y < min u a) = true →
    decide (a + w ≤ y && y < min u (a + 2 * w)) = false := by
  intro hP
  by_cases hQ : a + w ≤ y ∧ y < min u (a + 2 * w)
  · exfalso
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hP
    have hya : y < a := Nat.lt_of_lt_of_le hP.2 (Nat.min_le_right u a)
    omega
  · simp [hQ]

/-! ## Fan of the two adjacent buckets -/

private theorem fan_adjacent {u t v : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t v ys =
      (ys.filter (fun y => decide (v / (t + 1) + 1 == y / (t + 1) ||
                                  y / (t + 1) + 1 == v / (t + 1)))).length := by
  let w := t + 1
  let q := v / w
  let a := q * w
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  change ((opt (a - w) (min u a) ++ opt (a + w) (min u (a + 2 * w))).map
      (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
      (ys.filter (fun y => decide (q + 1 == y / w || y / w + 1 == q))).length
  rw [List.map_append, List.sum_append]
  have hterm1 :
      ((opt (a - w) (min u a)).map (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
        (ys.filter (fun y => decide (a - w ≤ y && y < min u a))).length := by
    by_cases h : a - w < min u a
    · rw [opt_some h]
      simp [lower_sub_eq_filter hs]
    · have hge : min u a ≤ a - w := by omega
      have hnil : ys.filter (fun y => decide (a ≤ y + w) && decide (y < min u a)) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro y hy hP
        simp only [decide_eq_true_eq, Bool.and_eq_true] at hP
        have hy2 : y < a - w := Nat.lt_of_lt_of_le hP.2 hge
        omega
      rw [opt_none h]
      simp [hnil]
  have hterm2 :
      ((opt (a + w) (min u (a + 2 * w))).map
        (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
        (ys.filter (fun y => decide (a + w ≤ y && y < min u (a + 2 * w)))).length := by
    by_cases h : a + w < min u (a + 2 * w)
    · rw [opt_some h]
      simp [lower_sub_eq_filter hs]
    · have hge : min u (a + 2 * w) ≤ a + w := by omega
      have hnil : ys.filter (fun y => decide (a + w ≤ y) && decide (y < min u (a + 2 * w))) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro y hy hP
        simp only [decide_eq_true_eq, Bool.and_eq_true] at hP
        omega
      rw [opt_none h]
      simp [hnil]
  rw [hterm1, hterm2]
  have hsum := filter_or_length
      (fun y => decide (a - w ≤ y && y < min u a))
      (fun y => decide (a + w ≤ y && y < min u (a + 2 * w))) ys
      (by
        intro y hy hP
        exact adjacent_pred_disjoint (u := u) (w := w) (a := a) (y := y) hP)
  rw [hsum]
  apply congrArg List.length
  apply List.filter_congr
  intro y hy
  have hiff := adjacent_test_iff (u := u) (w := w) (q := q) (a := a) (v := v) (y := y)
    (by omega) rfl (by simp [a, Nat.mul_comm]) (hv.2 y hy)
  apply Bool.eq_iff_iff.mpr
  simpa only [Bool.or_eq_true, decide_eq_true_eq, Bool.and_eq_true] using hiff

/-! ## Left outer multiplicity -/

theorem sound_LeftMultiplicity : AdjacentBucket.LeftMultiplicity.Goal := by
  intro u t xs ys hv
  let zs := ys.filterMap id
  have hfan : ∀ v : Nat, fan search u t v zs = (zs.filter (search.test t v)).length := by
    intro v
    rw [fan_adjacent hv]
    apply congrArg List.length
    apply List.filter_congr
    intro y hy
    simp only [search]
    by_cases h1 : v / (t + 1) + 1 = y / (t + 1) <;>
      by_cases h2 : y / (t + 1) + 1 = v / (t + 1) <;> simp [h1, h2]
  unfold zipOuter
  rw [List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro x hx
  cases x with
  | none => simp [Option.any]
  | some v =>
      dsimp
      rw [hfan v]
      by_cases he : (List.filter (fun y => search.test t v y) (List.filterMap id ys)).isEmpty
      · have hz : (List.filter (fun y => search.test t v y) (List.filterMap id ys)).length = 0 := by
          simpa using he
        simp [he, hz, zs]
      · have hle : 1 ≤ (List.filter (fun y => search.test t v y) (List.filterMap id ys)).length := by
          have hnz : ¬ (List.filter (fun y => search.test t v y) (List.filterMap id ys)).length = 0 := by
            intro hz
            exact he (by simpa using hz)
          omega
        simp [he, zs, Nat.max_eq_right hle]

-- truth check on small inputs (left keys including keys ≥ u, plus NULLs)
#eval
  let u := 6
  let leftOpts : List (Option Nat) := none :: (List.range 10).map some
  let xsList : List (List (Option Nat)) := [[]] ++ leftOpts.map (fun a => [a]) ++
    leftOpts.flatMap (fun a => leftOpts.map (fun b => [a, b]))
  let sorted : List (List Nat) := [[]] ++ (List.range u).map (fun a => [a]) ++
    (List.range u).flatMap (fun a => ((List.range u).filter (fun b => a ≤ b)).map (fun b => [a, b]))
  let ysList : List (List (Option Nat)) :=
    sorted.flatMap (fun l => [l.map some, none :: l.map some])
  (List.range 6).all (fun t =>
    xsList.all (fun xs => ysList.all (fun ys =>
      let zys := ys.filterMap id
      decide ((zipOuter search t xs ys).length =
          (xs.map (fun x => max 1 ((x.map (fun v => fan search u t v zys)).getD 0))).sum))))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_AdjacentBucket_LeftMultiplicity
