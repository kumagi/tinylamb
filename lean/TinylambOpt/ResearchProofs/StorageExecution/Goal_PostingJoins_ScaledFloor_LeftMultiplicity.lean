import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ScaledFloor.LeftMultiplicity` — Charge one left placeholder when zero right keys
  match, including a NULL left key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_LeftMultiplicity

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor
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

/-! ## ScaledFloor arithmetic -/

private def opt (a b : Nat) : Ranges :=
  TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b

private theorem scaled_test_iff {u w t y : Nat} (hw : 0 < w) (hyu : y < u) :
    (t * w ≤ y ∧ y < min u ((t + 1) * w)) ↔ y / w = t := by
  constructor
  · intro h
    apply Nat.le_antisymm
    · apply Nat.le_of_lt_succ
      exact (Nat.div_lt_iff_lt_mul hw).mpr
        (Nat.lt_of_lt_of_le h.2 (Nat.min_le_right u ((t + 1) * w)))
    · exact (Nat.le_div_iff_mul_le hw).mpr h.1
  · intro h
    constructor
    · have ht : t ≤ y / w := by
        rw [h]
        omega
      exact (Nat.le_div_iff_mul_le hw).mp ht
    · apply Nat.lt_min.mpr
      constructor
      · exact hyu
      · have hlt : y / w < t + 1 := by
          rw [h]
          omega
        exact (Nat.div_lt_iff_lt_mul hw).mp hlt

private theorem scaled_test_bool {u w t y : Nat} (hw : 0 < w) (hyu : y < u) :
    decide (t * w ≤ y && y < min u ((t + 1) * w)) = decide (y / w = t) := by
  have hiff := scaled_test_iff (u := u) (w := w) (t := t) (y := y) hw hyu
  by_cases hL : t * w ≤ y ∧ y < min u ((t + 1) * w)
  · have hQ : y / w = t := hiff.mp hL
    simp [hL, hQ]
  · have hQ : y / w ≠ t := by
      intro h
      exact hL (hiff.mpr h)
    simp [hL, hQ]

/-! ## Fan of the scaled-floor bucket -/

private theorem fan_scaled {u t v : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t v ys = (ys.filter (search.test t v)).length := by
  let w := v + 1
  let a := t * w
  have hw : 0 < w := by omega
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  change ((opt a (min u ((t + 1) * w))).map
      (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
      (ys.filter (fun y => decide (y / w = t))).length
  by_cases h : a < min u ((t + 1) * w)
  · have hopt : opt a (min u ((t + 1) * w)) = [(a, min u ((t + 1) * w))] := by
      simp [opt, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]
    rw [hopt]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
    rw [lower_sub_eq_filter hs]
    apply congrArg List.length
    apply List.filter_congr
    intro y hy
    simpa [a, beq_iff_eq] using
      scaled_test_bool (u := u) (w := w) (t := t) (y := y) hw (hv.2 y hy)
  · have hopt : opt a (min u ((t + 1) * w)) = [] := by
      simp [opt, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]
    rw [hopt]
    simp only [List.map_nil, List.sum_nil]
    have hnil2 : ys.filter (fun y => decide (y / w = t)) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro y hy hP
      have hEq : y / w = t := by simpa using hP
      have hyu : y < u := hv.2 y hy
      have ht : t ≤ y / w := by
        rw [hEq]
        omega
      have hgeY : t * w ≤ y := (Nat.le_div_iff_mul_le hw).mp ht
      have hltY' : y / w < t + 1 := by
        rw [hEq]
        omega
      have hltY : y < (t + 1) * w := (Nat.div_lt_iff_lt_mul hw).mp hltY'
      have hyMin : y < min u ((t + 1) * w) := Nat.lt_min.mpr ⟨hyu, hltY⟩
      omega
    rw [hnil2]
    simp

/-! ## Left outer multiplicity -/

theorem sound_LeftMultiplicity : ScaledFloor.LeftMultiplicity.Goal := by
  intro u t xs ys hv
  let zs := ys.filterMap id
  have hfan : ∀ v : Nat, fan search u t v zs = (zs.filter (search.test t v)).length := by
    intro v
    exact fan_scaled hv
  unfold zipOuter
  rw [List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro x hx
  cases x with
  | none => simp [Option.any]
  | some v =>
      simp only [Option.any, Option.map, Option.getD]
      let L := zs.filter (search.test t v)
      have hfanv : fan search u t v zs = L.length := by
        simpa [L] using hfan v
      change (if L.isEmpty then [(some v, none)] else L.map (fun y => (some v, some y))).length =
        max 1 (fan search u t v zs)
      by_cases he : L.isEmpty
      · have hz : L.length = 0 := by simpa using he
        simp [he, hz, hfanv]
      · have hpos : 0 < L.length := by
          by_cases hnil : L = []
          · exact False.elim (he (by simp [hnil]))
          · exact Nat.pos_of_ne_zero (by
              intro hz
              exact hnil (by simpa using hz))
        have hle : 1 ≤ L.length := by omega
        simp [he, hfanv, Nat.max_eq_right hle]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_LeftMultiplicity
