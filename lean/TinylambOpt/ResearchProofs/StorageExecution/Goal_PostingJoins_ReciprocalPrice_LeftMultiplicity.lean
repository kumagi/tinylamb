import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_LeftMultiplicity

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) :
    ys.Pairwise (· ≤ ·) := hv.1

private theorem valid_lt {u : Nat} {ys : List Nat} (hv : valid u ys) {y : Nat}
    (hy : y ∈ ys) : y < u := hv.2 y hy

/-! ## Binary lower bound -/

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
      simp only [searchBound]
      exact ⟨⟨hbelow, habove⟩, Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        exact ⟨⟨hbelow, habove⟩, by simp, by simp⟩
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
        · rw [if_pos hv]
          have hlt_mid : xs.toList.getD mid 0 < v := by
            rw [hlistval]
            exact hv
          have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases heq : j = mid
            · subst j
              simpa [getD_eq_getElem hmidlen] using hlt_mid
            · have hjm : j < mid := by omega
              have hjlen : j < xs.toList.length := by
                simpa using (by omega : j < xs.size)
              by_cases hjl : j < lo
              · exact hbelow j hjl
              · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                  simpa only [List.getD, List.getElem?_eq_getElem hjlen,
                    List.getElem?_eq_getElem hmidlen, Option.getD_some] using
                    hs.rel_getElem_of_lt hjlen hmidlen hjm
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ :=
            ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, by omega, hub⟩
        · rw [if_neg hv]
          have hge_mid : v ≤ xs.toList.getD mid 0 := by
            rw [hlistval]
            omega
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases heq : j = mid
            · subst j
              simpa [getD_eq_getElem hmidlen] using hge_mid
            · have hmj : mid < j := by omega
              by_cases hjh : hi ≤ j
              · exact habove j hjh hjlen
              · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                  simpa only [List.getD, List.getElem?_eq_getElem hmidlen,
                    List.getElem?_eq_getElem hjlen, Option.getD_some] using
                    hs.rel_getElem_of_lt hmidlen hjlen hmj
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ :=
            ih lo mid hml (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, by omega⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst n
    simp
  · have hne : n ≠ 0 := by
      intro hz
      simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hpow : (2 : Nat) ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by
      simp [Nat.pow_succ, Nat.mul_comm]
    rw [hpow]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    simpa using width_pow_bound ys.length
  have habove0 : ∀ j, ys.length ≤ j → j < ys.toArray.toList.length →
      v ≤ ys.toArray.toList.getD j 0 := by
    intro j hj hjlen
    have hj' : j < ys.length := by simpa using hjlen
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega) habove0 (by simpa using hs)
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
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) =
      ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_take]
    have hi_low : i < lower ys v := by
      simpa only [List.length_take, Nat.min_eq_left hle] using hi
    have hiys : i < ys.length := by omega
    have hlt : ys.getD i 0 < v := lower_lt_of_lt hs v hi_low
    rw [getD_eq_getElem hiys] at hlt
    exact hlt
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_drop]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := hi
      rw [List.length_drop] at hlen
      omega
    intro hlt
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) hiys
    rw [getD_eq_getElem hiys] at hge
    omega
  have hsplit := (List.take_append_drop (lower ys v) ys).symm
  calc
    lower ys v = (ys.take (lower ys v)).length := by
      rw [List.length_take]
      simp [Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter
          (fun x => decide (x < v))).length := by rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [← hsplit]

private theorem lt_min_iff_of_ltu {u b y : Nat} (hyu : y < u) :
    y < min u b ↔ y < b := by
  constructor
  · intro h
    exact Nat.lt_of_lt_of_le h (Nat.min_le_right u b)
  · intro h
    by_cases hb : b ≤ u
    · rw [Nat.min_eq_right hb]
      exact h
    · rw [Nat.min_eq_left (by omega)]
      exact hyu

/-! ## Counting matches for `ReciprocalPrice.search` -/

private theorem optional_eq (a b : Nat) : optional a b = (if a < b then [(a, b)] else []) := rfl

private theorem fan_eq_count (u t x : Nat) (ys : List Nat) (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  have hs := valid_pairwise hv
  unfold fan search
  dsimp
  rw [optional_eq]
  by_cases hpos : 0 < (if x == 0 then u else min u (t / x + 1))
  · rw [if_pos hpos]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
    have hl0 : lower ys 0 = 0 := by
      rw [lower_eq_filter_lt_length hs]
      simp
    rw [hl0, Nat.sub_zero]
    rw [lower_eq_filter_lt_length hs]
    rw [List.countP_eq_length_filter]
    apply congrArg List.length
    apply List.filter_congr
    intro y hy
    have hyu := valid_lt hv hy
    by_cases hx : x == 0
    · have hxzero : x = 0 := by simpa using hx
      subst x
      simp [hyu]
    · have hx0 : 0 < x := by cases x <;> simp_all
      simp only [hx, Bool.false_eq_true, ↓reduceIte]
      by_cases hylt : y < min u (t / x + 1)
      · have hylt' : y < t / x + 1 := (lt_min_iff_of_ltu hyu).mp hylt
        have hyle : y ≤ t / x := by omega
        have hmul : x * y ≤ t := by
          have hmul' : y * x ≤ t := (Nat.le_div_iff_mul_le hx0).mp hyle
          simpa [Nat.mul_comm] using hmul'
        simp [hylt, hmul]
      · have hmul : ¬ x * y ≤ t := by
          intro hmul
          have hmul' : y * x ≤ t := by simpa [Nat.mul_comm] using hmul
          have hyle : y ≤ t / x := (Nat.le_div_iff_mul_le hx0).mpr hmul'
          have hylt' : y < t / x + 1 := by omega
          exact hylt ((lt_min_iff_of_ltu hyu).mpr hylt')
        simp [hylt, hmul]
  · rw [if_neg hpos]
    simp only [List.map_nil, List.sum_nil]
    rw [List.countP_eq_length_filter]
    have hnil : (ys.filter (fun y => decide (x * y ≤ t))) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro y hy ht
      have hyu := valid_lt hv hy
      by_cases hx : x == 0
      · simp [hx] at hpos
        omega
      · have hx0 : 0 < x := by cases x <;> simp_all
        simp [hx] at hpos
        have hyle : y ≤ t / x := by
          have hmul' : y * x ≤ t := by simpa [Nat.mul_comm] using ht
          exact (Nat.le_div_iff_mul_le hx0).mpr hmul'
        have hylt : y < min u (t / x + 1) :=
          (lt_min_iff_of_ltu hyu).mpr (by omega)
        omega
    rw [hnil]
    simp

/-! ## Left multiplicity -/

private theorem leftMultiplicity_of_count
    (hcount : ∀ u t x ys, valid u ys →
      fan search u t x ys = ys.countP (search.test t x)) :
    leftMultiplicity search := by
  intro u t xs ys hv
  unfold zipOuter
  rw [List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro x hx
  cases x with
  | none => simp [Option.any, List.filter]
  | some v =>
      simp only [Option.any, Option.map_some, Option.getD_some]
      let rhs := (ys.filterMap id).filter (search.test t v)
      change (if rhs.isEmpty then [(some v, none)] else
        rhs.map (fun y => (some v, some y))).length =
        max 1 (fan search u t v (ys.filterMap id))
      have hcountv : fan search u t v (ys.filterMap id) = rhs.length := by
        have hc := hcount u t v (ys.filterMap id) hv
        rw [List.countP_eq_length_filter] at hc
        exact hc
      by_cases h : rhs.isEmpty
      · have hlen : rhs.length = 0 := by simpa using h
        rw [if_pos h, hcountv, hlen]
        simp
      · have hne : rhs.length ≠ 0 := by
          intro hz
          apply h
          simpa using hz
        have hlen : 0 < rhs.length := Nat.pos_of_ne_zero hne
        rw [if_neg h, hcountv]
        simp [Nat.max_eq_right (by omega : 1 ≤ rhs.length)]

theorem sound_LeftMultiplicity : LeftMultiplicity.Goal :=
  leftMultiplicity_of_count fan_eq_count

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_LeftMultiplicity
