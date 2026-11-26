import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `BestFrame.CrossFrame` — `∀ x y, measure (x++y) = max (max (measure x) (measure y))
  (max0 (suffs x)+max0 (prefs y))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_CrossFrame

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame

def vecs : Nat → List (List Int)
  | 0 => [[]]
  | n + 1 => (List.range 5).flatMap (fun i => (vecs n).map (fun xs => (Int.ofNat i - 2) :: xs))

-- Small exhaustive sanity check (all lists of length <= 4 over [-2, 2]).
#eval! (vecs 4).all (fun x => (vecs 4).all (fun y =>
  decide (measure (x ++ y) =
    max (max (measure x) (measure y)) (max0 (suffs x) + max0 (prefs y)))))

theorem le_max0 {l : List Int} {a : Int} (ha : a ∈ l) : a ≤ max0 l := by
  unfold max0
  cases h : l.max? with
  | none =>
      have hnil : l = [] := List.max?_eq_none_iff.mp h
      simp [hnil] at ha
  | some m =>
      have hm := (List.max?_eq_some_iff.mp h).2
      have ham : a ≤ m := hm a ha
      simp
      omega

theorem max0_le {l : List Int} {m : Int} (hm : 0 ≤ m)
    (hle : ∀ a ∈ l, a ≤ m) : max0 l ≤ m := by
  unfold max0
  cases h : l.max? with
  | none =>
      simp [hm]
  | some t =>
      have htmem := (List.max?_eq_some_iff.mp h).1
      have ht : t ≤ m := hle t htmem
      simp
      omega

theorem max0_eq_zero_or_pos (l : List Int) :
    max0 l = 0 ∨ ∃ a ∈ l, a = max0 l ∧ 0 < max0 l := by
  cases h : l.max? with
  | none =>
      left
      simp [max0, h]
  | some m =>
      have hmem := (List.max?_eq_some_iff.mp h).1
      by_cases hmpos : 0 < m
      · have hmnonneg : 0 ≤ m := by omega
        right
        refine ⟨m, hmem, ?_, ?_⟩
        · simp [max0, h] <;> omega
        · simp [max0, h] <;> omega
      · left
        simp [max0, h] <;> omega

theorem mem_windows_iff {z : Series} {w : Series} :
    w ∈ windows z ↔ ∃ i < z.length, ∃ j < z.length - i, w = (z.drop i).take (j + 1) := by
  constructor
  · intro h
    rcases List.mem_flatMap.mp h with ⟨i, hi, hwi⟩
    rcases List.mem_map.mp hwi with ⟨j, hj, hwj⟩
    rw [List.mem_range] at hi hj
    exact ⟨i, hi, j, hj, hwj.symm⟩
  · rintro ⟨i, hi, j, hj, rfl⟩
    apply List.mem_flatMap.mpr
    refine ⟨i, List.mem_range.mpr hi, ?_⟩
    apply List.mem_map.mpr
    exact ⟨j, List.mem_range.mpr hj, rfl⟩

theorem mem_windowSums_iff {z : Series} {s : Int} :
    s ∈ (windows z).map List.sum ↔
      ∃ i < z.length, ∃ j < z.length - i, ((z.drop i).take (j + 1)).sum = s := by
  constructor
  · intro h
    rcases List.mem_map.mp h with ⟨w, hw, hws⟩
    rcases (mem_windows_iff.mp hw) with ⟨i, hi, j, hj, rfl⟩
    exact ⟨i, hi, j, hj, hws⟩
  · rintro ⟨i, hi, j, hj, hs⟩
    apply List.mem_map.mpr
    refine ⟨(z.drop i).take (j + 1), ?_, hs⟩
    exact mem_windows_iff.mpr ⟨i, hi, j, hj, rfl⟩

theorem mem_suffs_iff {z : Series} {s : Int} :
    s ∈ suffs z ↔ ∃ i < z.length, (z.drop i).sum = s := by
  simp [suffs, List.mem_map, List.mem_range]

theorem mem_prefs_iff {z : Series} {s : Int} :
    s ∈ prefs z ↔ ∃ i < z.length, (z.take (i + 1)).sum = s := by
  simp [prefs, List.mem_map, List.mem_range]

theorem window_left_eq (x y : Series) (i j : Nat)
    (_hj : j < (x ++ y).length - i) (hij : i + j < x.length) :
    ((x ++ y).drop i).take (j + 1) = (x.drop i).take (j + 1) := by
  have hile : i ≤ x.length := by omega
  have htdrop : (x ++ y).drop i = x.drop i ++ y := List.drop_append_of_le_length hile
  rw [htdrop]
  apply List.take_append_of_le_length
  rw [List.length_drop]
  omega

theorem window_right_eq (x y : Series) (i j : Nat) (hix : x.length ≤ i) :
    ((x ++ y).drop i).take (j + 1) = (y.drop (i - x.length)).take (j + 1) := by
  rw [List.drop_append]
  have hxdrop : x.drop i = [] := by
    apply List.drop_eq_nil_of_le
    omega
  rw [hxdrop]
  simp

theorem cross_sum (x y : Series) (i j : Nat)
    (_hj : j < (x ++ y).length - i) (hxi : i < x.length) (hcross : x.length ≤ i + j) :
    (((x ++ y).drop i).take (j + 1)).sum =
      (x.drop i).sum + (y.take (i + j + 1 - x.length)).sum := by
  have hile : i ≤ x.length := by omega
  have htdrop : (x ++ y).drop i = x.drop i ++ y := List.drop_append_of_le_length hile
  rw [htdrop, List.take_append, List.sum_append, List.length_drop]
  have hsub : j + 1 - (x.length - i) = i + j + 1 - x.length := by omega
  rw [hsub]
  have htake : List.take (j + 1) (x.drop i) = x.drop i := by
    apply List.take_of_length_le
    rw [List.length_drop]
    omega
  rw [htake]

theorem max0_nonneg (l : List Int) : 0 ≤ max0 l := by
  unfold max0
  omega

theorem measure_nonneg (z : Series) : 0 ≤ BestFrame.measure z := by
  unfold BestFrame.measure
  exact max0_nonneg _

theorem window_le_measure {z : Series} {s : Int}
    (hs : s ∈ (windows z).map List.sum) : s ≤ BestFrame.measure z := by
  unfold BestFrame.measure
  exact le_max0 hs

theorem suffix_le_max0 {z : Series} (i : Nat) (hi : i < z.length) :
    (z.drop i).sum ≤ max0 (suffs z) := by
  apply le_max0
  exact mem_suffs_iff.mpr ⟨i, hi, rfl⟩

theorem prefix_le_max0 {z : Series} (k : Nat) (hkpos : 1 ≤ k) (hklen : k ≤ z.length) :
    (z.take k).sum ≤ max0 (prefs z) := by
  apply le_max0
  apply mem_prefs_iff.mpr
  refine ⟨k - 1, by omega, ?_⟩
  have : k - 1 + 1 = k := by omega
  rw [this]

theorem suffix_le_measure {z : Series} (i : Nat) (hi : i < z.length) :
    (z.drop i).sum ≤ BestFrame.measure z := by
  apply window_le_measure
  apply mem_windowSums_iff.mpr
  refine ⟨i, hi, z.length - i - 1, by omega, ?_⟩
  have hk : z.length - i - 1 + 1 = z.length - i := by omega
  rw [hk, List.take_of_length_le]
  rw [List.length_drop]
  omega

theorem prefix_le_measure {z : Series} (k : Nat) (hkpos : 1 ≤ k) (hklen : k ≤ z.length) :
    (z.take k).sum ≤ BestFrame.measure z := by
  apply window_le_measure
  apply mem_windowSums_iff.mpr
  refine ⟨0, by omega, k - 1, by omega, ?_⟩
  have hk : k - 1 + 1 = k := by omega
  rw [hk]
  simp

theorem measure_left_le (x y : Series) : BestFrame.measure x ≤ BestFrame.measure (x ++ y) := by
  have hlen : (x ++ y).length = x.length + y.length := by simp
  apply max0_le
  · exact measure_nonneg _
  · intro s hs
    apply le_max0
    rcases mem_windowSums_iff.mp hs with ⟨i, hi, j, hj, hs_eq⟩
    apply mem_windowSums_iff.mpr
    refine ⟨i, by omega, j, by omega, ?_⟩
    rw [window_left_eq x y i j (by omega) (by omega)]
    exact hs_eq

theorem measure_right_le (x y : Series) : BestFrame.measure y ≤ BestFrame.measure (x ++ y) := by
  have hlen : (x ++ y).length = x.length + y.length := by simp
  apply max0_le
  · exact measure_nonneg _
  · intro s hs
    apply le_max0
    rcases mem_windowSums_iff.mp hs with ⟨i, hi, j, hj, hs_eq⟩
    apply mem_windowSums_iff.mpr
    refine ⟨x.length + i, by omega, j, by omega, ?_⟩
    rw [window_right_eq x y (x.length + i) j (by omega)]
    have : x.length + i - x.length = i := by omega
    rw [this]
    exact hs_eq

theorem cross_le_measure (x y : Series) :
    max0 (suffs x) + max0 (prefs y) ≤ BestFrame.measure (x ++ y) := by
  have hlen : (x ++ y).length = x.length + y.length := by simp
  have hxy0 : 0 ≤ BestFrame.measure (x ++ y) := measure_nonneg _
  rcases max0_eq_zero_or_pos (suffs x) with hS0 | ⟨s, hs, hsval, hspos⟩
  · rcases max0_eq_zero_or_pos (prefs y) with hP0 | ⟨p, hp, hpval, hpp⟩
    · omega
    · have hp_le : p ≤ BestFrame.measure y := by
        rcases mem_prefs_iff.mp hp with ⟨i, hi, hpi⟩
        rw [← hpi]
        exact prefix_le_measure (z := y) (i + 1) (by omega) (by omega)
      have hy_le : BestFrame.measure y ≤ BestFrame.measure (x ++ y) := measure_right_le x y
      omega
  · rcases max0_eq_zero_or_pos (prefs y) with hP0 | ⟨p, hp, hpval, hpp⟩
    · have hs_le : s ≤ BestFrame.measure x := by
        rcases mem_suffs_iff.mp hs with ⟨i, hi, hsi⟩
        rw [← hsi]
        exact suffix_le_measure (z := x) i hi
      have hx_le : BestFrame.measure x ≤ BestFrame.measure (x ++ y) := measure_left_le x y
      omega
    · rcases mem_suffs_iff.mp hs with ⟨i, hi, hsi⟩
      rcases mem_prefs_iff.mp hp with ⟨t, ht, hpt⟩
      have hsum : (((x ++ y).drop i).take ((x.length - i) + t + 1)).sum = s + p := by
        have hc := cross_sum x y i ((x.length - i) + t) (by omega) hi (by omega)
        rw [hc, hsi]
        have hk : i + ((x.length - i) + t) + 1 - x.length = t + 1 := by omega
        rw [hk, hpt]
      have hle : s + p ≤ BestFrame.measure (x ++ y) := by
        apply window_le_measure
        apply mem_windowSums_iff.mpr
        refine ⟨i, by omega, (x.length - i) + t, by omega, hsum⟩
      omega

theorem sound_BestFrame_CrossFrame : BestFrame.CrossFrame.Goal := by
  intro x y
  have h1 : BestFrame.measure (x ++ y) ≤
      max (max (BestFrame.measure x) (BestFrame.measure y)) (max0 (suffs x) + max0 (prefs y)) := by
    change max0 ((windows (x ++ y)).map List.sum) ≤
      max (max (BestFrame.measure x) (BestFrame.measure y)) (max0 (suffs x) + max0 (prefs y))
    have hlen : (x ++ y).length = x.length + y.length := by simp
    have hmx : max0 ((windows x).map List.sum) = BestFrame.measure x := rfl
    have hmy : max0 ((windows y).map List.sum) = BestFrame.measure y := rfl
    apply max0_le
    · have hx0 : 0 ≤ max0 ((windows x).map List.sum) := max0_nonneg _
      have hy0 : 0 ≤ max0 ((windows y).map List.sum) := max0_nonneg _
      have hsx0 : 0 ≤ max0 (suffs x) := max0_nonneg _
      have hpy0 : 0 ≤ max0 (prefs y) := max0_nonneg _
      omega
    · intro s hs
      rcases mem_windowSums_iff.mp hs with ⟨i, hi, j, hj, hs_eq⟩
      by_cases hij : i + j < x.length
      · have heq := window_left_eq x y i j (by omega) hij
        have hle : ((x.drop i).take (j + 1)).sum ≤ max0 ((windows x).map List.sum) := by
          apply le_max0
          exact mem_windowSums_iff.mpr ⟨i, by omega, j, by omega, rfl⟩
        rw [heq] at hs_eq
        omega
      · by_cases hix : i < x.length
        · have hc := cross_sum x y i j (by omega) hix (by omega)
          rw [hc] at hs_eq
          have hsle : (x.drop i).sum ≤ max0 (suffs x) := suffix_le_max0 (z := x) i hix
          have hple : (y.take (i + j + 1 - x.length)).sum ≤ max0 (prefs y) :=
            prefix_le_max0 (z := y) (i + j + 1 - x.length) (by omega) (by omega)
          omega
        · have heq := window_right_eq x y i j (by omega)
          rw [heq] at hs_eq
          have hle : ((y.drop (i - x.length)).take (j + 1)).sum ≤
              max0 ((windows y).map List.sum) := by
            apply le_max0
            exact mem_windowSums_iff.mpr ⟨i - x.length, by omega, j, by omega, rfl⟩
          omega
  have h2 : max (max (BestFrame.measure x) (BestFrame.measure y)) (max0 (suffs x) + max0 (prefs y)) ≤
      BestFrame.measure (x ++ y) := by
    have hx_le : BestFrame.measure x ≤ BestFrame.measure (x ++ y) := measure_left_le x y
    have hy_le : BestFrame.measure y ≤ BestFrame.measure (x ++ y) := measure_right_le x y
    have hc_le : max0 (suffs x) + max0 (prefs y) ≤ BestFrame.measure (x ++ y) := cross_le_measure x y
    omega
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_CrossFrame
