import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_LeftMultiplicity

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) :
    ys.Pairwise (· ≤ ·) := hv.1

/-! ## Binary lower bound -/

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
    simp only [decide_eq_true_eq, List.getElem_take]
    have hi' : i < lower ys v := by omega
    have hiys : i < ys.length := by omega
    have : ys.getD i 0 < v := lower_lt_of_lt hs v hi'
    rwa [getD_eq_getElem hiys] at this
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

/-! ## Counting matches for `SharedPrefix.search` -/

private theorem fan_eq_count (u t x : Nat) (ys : List Nat) (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  let w := 2 ^ t
  let a := x / w * w
  have hw : 0 < w := by
    dsimp [w]
    exact Nat.pow_pos (by omega : 0 < 2)
  have hs := valid_pairwise hv
  unfold fan search
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional]
  change ((if a < min u (a + w) then [(a, min u (a + w))] else []).map
      (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
    ys.countP (fun y => x / w == y / w)
  by_cases hlt : a < min u (a + w)
  · rw [ite_eq_left hlt]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
    rw [lower_eq_filter_lt_length hs (min u (a + w)), lower_eq_filter_lt_length hs a]
    have hab : a ≤ min u (a + w) := Nat.le_of_lt hlt
    rw [filter_lt_prefix_filter_lt hs hab]
    rw [List.countP_eq_length_filter]
    simp only [List.length_append, Nat.add_sub_cancel_left]
    apply congrArg List.length
    apply List.filter_congr
    intro y hy
    have hyu := hv.2 y hy
    have hmin : y < min u (a + w) ↔ y < a + w := lt_min_iff_of_ltu hyu
    by_cases hay : a ≤ y
    · by_cases hya : y < min u (a + w)
      · have hylt : y < a + w := hmin.mp hya
        have hxwy : x / w = y / w := by
          apply Nat.le_antisymm
          · rw [Nat.le_div_iff_mul_le hw]
            simpa [a] using hay
          · have hmul : (x / w + 1) * w = a + w := by simp [a, Nat.succ_mul]
            have hylt' : y < (x / w + 1) * w := by simpa [hmul] using hylt
            have hdiv : y / w < x / w + 1 := (Nat.div_lt_iff_lt_mul hw).mpr hylt'
            omega
        simp [hay, hya, hxwy]
      · have hxwy : x / w ≠ y / w := by
          intro heq
          have hybound : y < (y / w) * w + w := Nat.lt_div_mul_add hw
          rw [← heq] at hybound
          have : y < a + w := by simpa [a] using hybound
          exact hya (hmin.mpr this)
        simp [hay, hya, hxwy]
    · have hxwy : x / w ≠ y / w := by
        intro heq
        have hybound : (y / w) * w ≤ y := Nat.div_mul_le_self y w
        rw [← heq] at hybound
        exact hay (by simpa [a] using hybound)
      simp [hay, hxwy]

  · rw [ite_eq_right hlt]
    simp only [List.map_nil, List.sum_nil]
    rw [List.countP_eq_length_filter]
    have hnil : ys.filter (fun y => x / w == y / w) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro y hy ht
      simp only [beq_iff_eq] at ht
      have hyu := hv.2 y hy
      have hay : a ≤ y := by
        have hle : (y / w) * w ≤ y := Nat.div_mul_le_self y w
        rw [← ht] at hle
        simpa [a] using hle
      have hya : y < min u (a + w) := (lt_min_iff_of_ltu hyu).mpr (by
        have hybound : y < (y / w) * w + w := Nat.lt_div_mul_add hw
        rw [← ht] at hybound
        simpa [a] using hybound)
      exact hlt (Nat.lt_of_le_of_lt hay hya)
    rw [hnil]
    simp

/-! ## Left multiplicity -/

private theorem leftMultiplicity_of_count
    (hcount : ∀ u t x ys, valid u ys → fan search u t x ys = ys.countP (search.test t x)) :
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
    by_cases h : rhs.isEmpty
    · have hlen : rhs.length = 0 := by simpa using h
      rw [show ((ys.filterMap id).filter (search.test t v)).isEmpty = true by simpa [rhs] using h]
      simp only [↓reduceIte]
      have hc := hcount u t v (ys.filterMap id) hv
      rw [List.countP_eq_length_filter] at hc
      change 1 = max 1 (fan search u t v (ys.filterMap id))
      rw [hc]
      change 1 = max 1 rhs.length
      simp [hlen]
    · rw [show ((ys.filterMap id).filter (search.test t v)).isEmpty = false by simpa [rhs] using h]
      rw [ite_eq_right (by decide : ¬ (false = true))]
      rw [List.length_map]
      have hlen : 0 < rhs.length := by
        cases hr : rhs with
        | nil =>
            exfalso
            apply h
            simp [hr]
        | cons y ys => simp
      have hc := hcount u t v (ys.filterMap id) hv
      rw [List.countP_eq_length_filter] at hc
      have hmax : max 1 rhs.length = rhs.length := Nat.max_eq_right (by omega)
      change rhs.length = max 1 (fan search u t v (ys.filterMap id))
      rw [hc, hmax]

theorem sound_LeftMultiplicity : LeftMultiplicity.Goal :=
  leftMultiplicity_of_count fan_eq_count

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_LeftMultiplicity
