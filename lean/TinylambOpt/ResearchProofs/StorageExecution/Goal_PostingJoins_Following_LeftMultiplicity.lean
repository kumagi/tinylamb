import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Following.LeftMultiplicity` — Charge one left placeholder when zero right keys
  match, including a NULL left key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_LeftMultiplicity

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following
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
      (Nat.log2_lt (n := n) (k := n.log2 + 1) hn).mp (by omega)
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

/-! ## Fan of a forward interval -/

private theorem optional_eq (a b : Nat) : optional a b = if a < b then [(a, b)] else [] := rfl

private theorem fan_following {u t v : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t v ys = (ys.filter (fun y => decide (v < y && y ≤ v + t))).length := by
  have hs := hv.1
  unfold fan search
  by_cases h : v + 1 < min u (v + t + 1)
  · simp [optional_eq, h]
    rw [lower_sub_eq_filter hs (v + 1) (min u (v + t + 1))]
    apply congrArg List.length
    apply List.filter_congr
    intro y hy
    have hyu : y < u := hv.2 y hy
    have hmin : y < min u (v + t + 1) ↔ y < v + t + 1 := by
      constructor
      · intro hy'
        exact Nat.lt_of_lt_of_le hy' (Nat.min_le_right _ _)
      · intro hy'
        exact Nat.lt_min.mpr ⟨hyu, hy'⟩
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro hh
      have hh' : v + 1 ≤ y ∧ y < min u (v + t + 1) := by simpa using hh
      have hyw : y < v + t + 1 :=
        Nat.lt_of_lt_of_le hh'.2 (Nat.min_le_right _ _)
      simpa [Bool.and_eq_true] using (show v < y ∧ y ≤ v + t by omega)
    · intro hh
      have hh' : v < y ∧ y ≤ v + t := by simpa [Bool.and_eq_true] using hh
      have hyw : y < v + t + 1 := by omega
      simpa using (show v + 1 ≤ y ∧ y < min u (v + t + 1) from
        ⟨by omega, Nat.lt_min.mpr ⟨hyu, hyw⟩⟩)
  · simp [optional_eq, h]
    have hnil : ys.filter (fun y => decide (v < y) && decide (y ≤ v + t)) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro y hy
      have hyu : y < u := hv.2 y hy
      simp only [Bool.and_eq_true]
      intro hvy
      have hvylt : v < y := by simpa using hvy.1
      have hyvle : y ≤ v + t := by simpa using hvy.2
      have hcontra : v + 1 < min u (v + t + 1) := by
        apply Nat.lt_min.mpr
        constructor <;> omega
      exact h hcontra
    rw [hnil]
    rfl

/-! ## Left outer multiplicity -/

theorem sound_LeftMultiplicity : Following.LeftMultiplicity.Goal := by
  intro u t xs ys hv
  let zs := ys.filterMap id
  have hfan : ∀ v : Nat,
      fan search u t v zs = (zs.filter (fun y => decide (v < y && y ≤ v + t))).length := by
    intro v
    exact fan_following hv
  unfold zipOuter
  rw [List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro x hx
  cases x with
  | none =>
      simp [Option.any]
  | some v =>
      have hfilter : zs.filter (fun y => decide (v < y && y ≤ v + t)) =
          zs.filter (search.test t v) := by
        apply List.filter_congr
        intro y hy
        by_cases h1 : v < y <;> by_cases h2 : y ≤ v + t <;> simp [search, h1, h2]
      have hcount : (zs.filter (search.test t v)).length = fan search u t v zs := by
        rw [← hfilter, ← hfan v]
      let L := zs.filter (search.test t v)
      have houter :
          (let hits := zs.filter (search.test t v);
             if hits.isEmpty then [(some v, none)] else hits.map (fun y => (some v, some y))).length =
            (if L.isEmpty then 1 else L.length) := by
        dsimp [L]
        by_cases he : (zs.filter (search.test t v)).isEmpty <;> simp [he]
      simp only [Option.any, Option.map_some, Option.getD_some]
      rw [houter, ← hcount]
      change (if L.isEmpty then 1 else L.length) = max 1 L.length
      by_cases he : L.isEmpty
      · have hz : L.length = 0 := by simpa using he
        simp [he, hz]
      · have hp : 0 < L.length := by
          apply Nat.pos_of_ne_zero
          intro hz
          apply he
          simpa using hz
        rw [if_neg he, Nat.max_eq_right (by omega)]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_LeftMultiplicity
