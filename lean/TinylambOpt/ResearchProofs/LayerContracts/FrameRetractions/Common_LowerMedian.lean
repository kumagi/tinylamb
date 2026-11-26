import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.CommonLowerMedian

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open List

/-- The Bool comparator used by `lowerMedian`. -/
abbrev leNat : Nat → Nat → Bool := fun a b => decide (a ≤ b)

theorem pairwise_ms (l : List Nat) : Pairwise (· ≤ ·) (l.mergeSort leNat) := by
  simpa [leNat] using
    (pairwise_mergeSort
      (le := leNat)
      (by
        intro a b c ha hb
        simp [leNat] at ha hb ⊢
        omega)
      (by
        intro a b
        by_cases h : a ≤ b
        · simp [leNat, h]
        · have hba : b ≤ a := Nat.le_of_not_ge h
          simp [leNat, h, hba]) l)

theorem ms_length (l : List Nat) : (l.mergeSort leNat).length = l.length :=
  length_mergeSort l

theorem ms_perm (l : List Nat) : (l.mergeSort leNat).Perm l := mergeSort_perm l leNat

theorem ms_invariant {a b : List Nat} (p : a.Perm b) : a.mergeSort leNat = b.mergeSort leNat :=
  Perm.eq_of_pairwise
    (fun q r _ _ hqr hrq => Nat.le_antisymm hqr hrq)
    (pairwise_ms a) (pairwise_ms b)
    ((ms_perm a).trans (p.trans (ms_perm b).symm))

theorem lowerMedian_of_perm {a b : List Nat} (p : a.Perm b) : lowerMedian a = lowerMedian b := by
  unfold lowerMedian
  rw [ms_invariant p, p.length_eq]

theorem getD_eq_getElem {xs : List Nat} {n d : Nat} (h : n < xs.length) :
    xs[n]?.getD d = xs[n] := by
  rw [List.getElem?_eq_getElem h, Option.getD_some]

theorem perm_countP {P : Nat → Bool} {a b : List Nat} (p : a.Perm b) :
    a.countP P = b.countP P := by
  induction p with
  | nil => rfl
  | cons x p ih => simp [List.countP_cons, ih]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂
  | swap x y l =>
      by_cases hx : P x = true <;> by_cases hy : P y = true <;>
        simp [List.countP_cons, hx, hy, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

theorem ms_count {xs : List Nat} (P : Nat → Bool) : (xs.mergeSort leNat).countP P = xs.countP P :=
  perm_countP (ms_perm xs)

theorem countP_zero {P : Nat → Bool} (s : List Nat) (h : ∀ y ∈ s, P y = false) :
    s.countP P = 0 := by
  induction s with
  | nil => rfl
  | cons a t ih =>
      rw [List.countP_cons_of_neg (by simpa using h a (by simp))]
      exact ih (fun y hy => h y (by simp [hy]))

/-- On a sorted list, a down-closed predicate holds on the whole prefix up to `i`. -/
theorem countP_prefix {P : Nat → Bool}
    (hmono : ∀ x y : Nat, x ≤ y → P y = true → P x = true) :
    ∀ s : List Nat, Pairwise (· ≤ ·) s →
      ∀ (i : Nat) (hi : i < s.length), P (s[i]'hi) = true → i + 1 ≤ s.countP P := by
  intro s
  induction s with
  | nil =>
      intro hs i hi hP
      simp at hi
  | cons a t ih =>
      intro hs i hi hP
      cases i with
      | zero =>
          have hPa : P a = true := by simpa using hP
          rw [List.countP_cons_of_pos hPa]
          omega
      | succ j =>
          obtain ⟨hhead, hts⟩ := List.pairwise_cons.mp hs
          have hj : j < t.length := by simpa using hi
          have haj : a ≤ t[j] := hhead _ (List.getElem_mem hj)
          have hPj : P t[j] = true := by simpa using hP
          have ha : P a = true := hmono a (t[j]) haj hPj
          rw [List.countP_cons_of_pos ha]
          have hj' := ih hts j hj hPj
          omega

/-- On a sorted list, a down-closed predicate fails on the whole suffix from `i`. -/
theorem countP_suffix {P : Nat → Bool}
    (hmono : ∀ x y : Nat, x ≤ y → P y = true → P x = true) :
    ∀ s : List Nat, Pairwise (· ≤ ·) s →
      ∀ (i : Nat) (hi : i < s.length), P (s[i]'hi) = false → s.countP P ≤ i := by
  intro s
  induction s with
  | nil =>
      intro hs i hi hP
      simp at hi
  | cons a t ih =>
      intro hs i hi hP
      cases i with
      | zero =>
          have hPa : P a = false := by simpa using hP
          obtain ⟨hhead, hts⟩ := List.pairwise_cons.mp hs
          have hall : ∀ y ∈ t, P y = false := by
            intro y hy
            by_cases hPy : P y = true
            · have hPa' : P a = true := hmono a y (hhead y hy) hPy
              rw [hPa'] at hPa
              contradiction
            · simpa using hPy
          rw [List.countP_cons_of_neg (by simpa using hPa), countP_zero t hall]
          omega
      | succ j =>
          obtain ⟨_, hts⟩ := List.pairwise_cons.mp hs
          have hj : j < t.length := by simpa using hi
          have hPj : P t[j] = false := by simpa using hP
          have hj' := ih hts j hj hPj
          rw [List.countP_cons]
          split <;> omega

theorem countP_erase_of_mem {P : Nat → Bool} {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    xs.countP P = (xs.erase x).countP P + if P x = true then 1 else 0 := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  by_cases hp : P x = true
  · simp [hp]
    have hmem : x ∈ xs.filter P := by simp [List.mem_filter, hx, hp]
    have hpos : 0 < (xs.filter P).length := List.length_pos_of_mem hmem
    have hlen := List.length_erase_of_mem hmem
    rw [← (List.erase_filter (f := P) (a := x) (l := xs))]
    omega
  · have heq : xs.filter P = (xs.erase x).filter P := by
      induction xs with
      | nil => simp at hx
      | cons a t ih =>
          by_cases ha : a == x
          · have hax : a = x := by simpa using ha
            subst a
            simp [hp]
          · have hane : ¬ a == x := by simpa using ha
            have hax : a ≠ x := by intro h; subst x; exact hane (by simp)
            have hxt : x ∈ t := by
              simp only [List.mem_cons] at hx
              rcases hx with h | h
              · exact False.elim (hax h.symm)
              · exact h
            rw [List.erase_cons_tail hane]
            rw [List.filter_cons, List.filter_cons, ih hxt]
    rw [heq]
    simp [hp]

/-- Count bounds pinning the lower median at the canonical index. -/
theorem bounds {xs : List Nat} {v : Nat} (hpos : 0 < xs.length) (hmed : lowerMedian xs = v) :
    xs.countP (fun z => decide (z < v)) ≤ (xs.length - 1) / 2 ∧
      (xs.length - 1) / 2 + 1 ≤ xs.countP (fun z => decide (z ≤ v)) := by
  have hlt : (xs.length - 1) / 2 < (xs.mergeSort leNat).length := by
    rw [ms_length]
    omega
  have hget : (xs.mergeSort leNat)[(xs.length - 1) / 2] = v := by
    unfold lowerMedian at hmed
    rw [getD_eq_getElem hlt] at hmed
    exact hmed
  refine ⟨?_, ?_⟩
  · have key : (fun z => decide (z < v)) (xs.mergeSort leNat)[(xs.length - 1) / 2] = false := by
      rw [hget]
      simp
    have hb := countP_suffix (P := fun z => decide (z < v)) (fun x y hxy hy => by
      simp only [decide_eq_true_eq] at hy ⊢
      omega) (xs.mergeSort leNat) (pairwise_ms xs) ((xs.length - 1) / 2) hlt key
    rw [ms_count] at hb
    exact hb
  · have key : (fun z => decide (z ≤ v)) (xs.mergeSort leNat)[(xs.length - 1) / 2] = true := by
      rw [hget]
      simp
    have hb := countP_prefix (P := fun z => decide (z ≤ v)) (fun x y hxy hy => by
      simp only [decide_eq_true_eq] at hy ⊢
      omega) (xs.mergeSort leNat) (pairwise_ms xs) ((xs.length - 1) / 2) hlt key
    rw [ms_count] at hb
    exact hb

/-- If the count bounds pin `m`, that value is the lower median at index `t`. -/
theorem lowerMedian_eq_of_bounds {xs : List Nat} {m t : Nat}
    (ht : t = (xs.length - 1) / 2) (htl : t < xs.length)
    (h1 : xs.countP (fun z => decide (z < m)) ≤ t)
    (h2 : t + 1 ≤ xs.countP (fun z => decide (z ≤ m))) :
    lowerMedian xs = m := by
  have hs : Pairwise (· ≤ ·) (xs.mergeSort leNat) := pairwise_ms xs
  have hlt : t < (xs.mergeSort leNat).length := by
    rw [ms_length]
    exact htl
  unfold lowerMedian
  rw [← ht, getD_eq_getElem hlt]
  by_cases hltm : (xs.mergeSort leNat)[t] < m
  · have key : (fun z => decide (z < m)) (xs.mergeSort leNat)[t] = true := by simp [hltm]
    have hge := countP_prefix (P := fun z => decide (z < m)) (fun x y hxy hy => by
      simp only [decide_eq_true_eq] at hy ⊢
      omega) (xs.mergeSort leNat) hs t hlt key
    have hb1 : (xs.mergeSort leNat).countP (fun z => decide (z < m)) ≤ t := by
      rw [ms_count]
      exact h1
    omega
  · by_cases hgtm : m < (xs.mergeSort leNat)[t]
    · have key : (fun z => decide (z ≤ m)) (xs.mergeSort leNat)[t] = false := by simp [hgtm]
      have hle := countP_suffix (P := fun z => decide (z ≤ m)) (fun x y hxy hy => by
        simp only [decide_eq_true_eq] at hy ⊢
        omega) (xs.mergeSort leNat) hs t hlt key
      have hb2 : t + 1 ≤ (xs.mergeSort leNat).countP (fun z => decide (z ≤ m)) := by
        rw [ms_count]
        exact h2
      omega
    · omega

theorem append_t_le (a b : Nat) : (a - 1) / 2 + (b - 1) / 2 ≤ (a + b - 1) / 2 := by
  omega

theorem append_t_ge (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    (a + b - 1) / 2 + 1 ≤ (a - 1) / 2 + (b - 1) / 2 + 2 := by
  omega

theorem double_t (n : Nat) : (n + n - 1) / 2 = n - 1 := by
  omega

/-- The lower median of a concatenation whose both lower medians agree is that value. -/
theorem lowerMedian_append_of_eq {a b : List Nat} {v : Nat}
    (ha : a ≠ []) (hb : b ≠ []) (hva : lowerMedian a = v) (hvb : lowerMedian b = v) :
    lowerMedian (a ++ b) = v := by
  have hla : 0 < a.length := (List.length_pos_iff).mpr ha
  have hlb : 0 < b.length := (List.length_pos_iff).mpr hb
  have htl : ((a ++ b).length - 1) / 2 < (a ++ b).length := by
    rw [length_append]
    omega
  apply lowerMedian_eq_of_bounds (t := ((a ++ b).length - 1) / 2) rfl htl
  · rw [List.countP_append, length_append]
    have ht := append_t_le a.length b.length
    obtain ⟨h1a, _⟩ := bounds hla hva
    obtain ⟨h1b, _⟩ := bounds hlb hvb
    exact Nat.le_trans (Nat.add_le_add h1a h1b) ht
  · rw [List.countP_append, length_append]
    have ht := append_t_ge a.length b.length hla hlb
    obtain ⟨_, h2a⟩ := bounds hla hva
    obtain ⟨_, h2b⟩ := bounds hlb hvb
    have hsum : (a.length - 1) / 2 + (b.length - 1) / 2 + 2 ≤
        a.countP (fun z => decide (z ≤ v)) + b.countP (fun z => decide (z ≤ v)) := by
      omega
    exact Nat.le_trans ht hsum

/-- Doubling a nonempty bag preserves its lower median. -/
theorem lowerMedian_double {xs : List Nat} (hxs : xs ≠ []) : lowerMedian (xs ++ xs) = lowerMedian xs := by
  have hpos : 0 < xs.length := (List.length_pos_iff).mpr hxs
  obtain ⟨h1, h2⟩ := bounds hpos (rfl : lowerMedian xs = lowerMedian xs)
  have htl : ((xs ++ xs).length - 1) / 2 < (xs ++ xs).length := by
    rw [length_append]
    omega
  apply lowerMedian_eq_of_bounds (t := ((xs ++ xs).length - 1) / 2) rfl htl (m := lowerMedian xs)
  · rw [List.countP_append, length_append, double_t]
    omega
  · rw [List.countP_append, length_append, double_t]
    omega

theorem fastRetraction_sound : LowerMedian.FastRetraction.Goal := by
  intro xs x out hx hd
  by_cases hlen : xs.length == 1
  · have hn : xs.length = 1 := by simpa using hlen
    have hxs : xs = [x] := by
      cases xs with
      | nil => simp at hn
      | cons y ys =>
          simp at hn
          subst ys
          have hxy : x = y := by simpa using hx
          subst y
          rfl
    subst xs
    simp [dropOne, value] at hd ⊢
    exact hd
  · have hlenb : (xs.length == 1) = false := by simpa using hlen
    have hpos : 0 < xs.length := List.length_pos_of_mem hx
    let v := lowerMedian xs
    have hv : v = lowerMedian xs := rfl
    cases hrm : LowerMedian.model.remove xs.length (LowerMedian.model.result xs) x with
    | none =>
        rw [dropOne, hlenb, hrm] at hd
        exact absurd hd (by simp)
    | some w =>
        have hout : some w = out := by simpa [dropOne, hlenb, hrm] using hd
        have hrm' : (if xs.length % 2 == 0 && x > v then some v
            else if xs.length % 2 == 1 && x < v then some v else none) = some w := by
          simpa [LowerMedian.model, ← hv] using hrm
        have hlene : (xs.erase x).length = xs.length - 1 := List.length_erase_of_mem hx
        have hlen_ne_one : xs.length ≠ 1 := by
          intro h1
          apply hlen
          simp [h1]
        have hlen_ge_two : 2 ≤ xs.length := by omega
        have hne : xs.erase x ≠ [] := by
          intro h
          have hl := congrArg List.length h
          rw [hlene] at hl
          simp at hl
          omega
        obtain ⟨h1b, h2b⟩ := bounds hpos hv.symm
        have hmed : lowerMedian (xs.erase x) = w := by
          rcases Nat.mod_two_eq_zero_or_one xs.length with hmod | hmod
          · -- even population: x is strictly above the median; the slot is unchanged
            have hif : (if x > v then some v else none) = some w := by simpa [hmod] using hrm'
            by_cases hxg : x > v
            · rw [if_pos hxg] at hif
              have hrew : w = v := (Option.some.inj hif).symm
              have hnotlt : (fun z => decide (z < v)) x = false := by
                simp [Nat.not_lt.mpr (Nat.le_of_lt hxg)]
              have hnotle : (fun z => decide (z ≤ v)) x = false := by
                simp [Nat.not_le.mpr hxg]
              have hc1base := countP_erase_of_mem (P := fun z => decide (z < v)) hx
              have hc2base := countP_erase_of_mem (P := fun z => decide (z ≤ v)) hx
              simp [hnotlt] at hc1base
              simp [hnotle] at hc2base
              have hltl : ((xs.erase x).length - 1) / 2 < (xs.erase x).length := by
                rw [hlene]
                omega
              have hm := lowerMedian_eq_of_bounds (xs := xs.erase x) (m := v)
                (t := ((xs.erase x).length - 1) / 2) rfl hltl
              have htE : ((xs.erase x).length - 1) / 2 = (xs.length - 1) / 2 := by
                rw [hlene]
                omega
              have hb1 : (xs.erase x).countP (fun z => decide (z < v)) ≤
                  ((xs.erase x).length - 1) / 2 := by
                rw [← hc1base, htE]
                exact h1b
              have hb2 : ((xs.erase x).length - 1) / 2 + 1 ≤
                  (xs.erase x).countP (fun z => decide (z ≤ v)) := by
                rw [← hc2base, htE]
                exact h2b
              exact (hm hb1 hb2).trans hrew.symm
            · rw [if_neg hxg] at hif
              exact absurd hif (by simp)
          · -- odd population: x is strictly below the median; the slot keeps its value
            have hif : (if x < v then some v else none) = some w := by simpa [hmod] using hrm'
            by_cases hxl : x < v
            · rw [if_pos hxl] at hif
              have hrew : w = v := (Option.some.inj hif).symm
              have hlt : (fun z => decide (z < v)) x = true := by simp [hxl]
              have hle : (fun z => decide (z ≤ v)) x = true := by simp [Nat.le_of_lt hxl]
              have hc1base := countP_erase_of_mem (P := fun z => decide (z < v)) hx
              have hc2base := countP_erase_of_mem (P := fun z => decide (z ≤ v)) hx
              simp [hlt] at hc1base
              simp [hle] at hc2base
              have hltl : ((xs.erase x).length - 1) / 2 < (xs.erase x).length := by
                rw [hlene]
                omega
              have htE : ((xs.erase x).length - 1) / 2 + 1 = (xs.length - 1) / 2 := by
                rw [hlene]
                omega
              have hb1 : (xs.erase x).countP (fun z => decide (z < v)) ≤
                  ((xs.erase x).length - 1) / 2 := by omega
              have hb2 : ((xs.erase x).length - 1) / 2 + 1 ≤
                  (xs.erase x).countP (fun z => decide (z ≤ v)) := by omega
              have hm := lowerMedian_eq_of_bounds (xs := xs.erase x) (m := v)
                (t := ((xs.erase x).length - 1) / 2) rfl hltl
              exact (hm hb1 hb2).trans hrew.symm
            · rw [if_neg hxl] at hif
              exact absurd hif (by simp)
        simpa [value, LowerMedian.model, List.isEmpty_eq_false_iff.mpr hne, hmed] using hout



theorem mergeSort_two (x y : Nat) : [x, y].mergeSort leNat = [min x y, max x y] := by
  rw [mergeSort_pair]
  by_cases h : x ≤ y
  · simp [leNat, h, Nat.min_eq_left h, Nat.max_eq_right h]
  · have hyx : y ≤ x := Nat.le_of_not_ge h
    simp [leNat, h, hyx, Nat.min_eq_right hyx, Nat.max_eq_left hyx]

theorem lowerMedian_pair (x y : Nat) : lowerMedian [x, y] = min x y := by
  unfold lowerMedian
  rw [mergeSort_two, show ((([x, y] : List Nat).length - 1) / 2) = 0 from by simp]
  rw [getD_eq_getElem (by simp : (0 : Nat) < [min x y, max x y].length)]
  simp


end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.CommonLowerMedian
