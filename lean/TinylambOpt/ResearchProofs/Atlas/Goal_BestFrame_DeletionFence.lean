import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_DeletionFence

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Exhaustive sanity check: all lists of length ≤ 4 over [-2, 2], and all
    predicates on that value set. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec vecs : Nat → List (List Int)
    | 0 => [[]]
    | k + 1 => vecs k ++ vals.flatMap (fun v => (vecs k).map (fun xs => v :: xs))
  let preds : List (Nat × (Int → Bool)) :=
    (List.range 32).map (fun bits : Nat =>
      (bits, fun v : Int => ((bits >>> (vals.idxOf v)) % 2) == 1))
  let bad := (vecs 4).flatMap (fun (x : List Int) =>
    preds.filterMap (fun (p : Nat × (Int → Bool)) =>
      let keep := p.2
      let lhs := BestFrame.measure (x.filter keep)
      let rhs := BestFrame.measure x +
        ((x.filter (fun v => !keep v)).map (fun v => max 0 (-v))).sum
      if lhs > rhs then some (x, p.1, lhs, rhs) else none))
  pure (bad.length)

private def negSum (x : Series) : Int := (x.map (fun v => max 0 (-v))).sum

private theorem max0_nonneg (l : List Int) : 0 ≤ max0 l := by
  unfold max0
  omega

private theorem le_max0 {l : List Int} {a : Int} (ha : a ∈ l) : a ≤ max0 l := by
  unfold max0
  cases h : l.max? with
  | none =>
      have hnil : l = [] := List.max?_eq_none_iff.mp h
      simp [hnil] at ha
  | some m =>
      have hm := (List.max?_eq_some_iff.mp h).2
      have ham := hm a ha
      simp
      omega

private theorem max0_le {l : List Int} {m : Int} (hm : 0 ≤ m)
    (hle : ∀ a ∈ l, a ≤ m) : max0 l ≤ m := by
  unfold max0
  cases h : l.max? with
  | none => simp [hm]
  | some t =>
      have htmem := (List.max?_eq_some_iff.mp h).1
      have ht := hle t htmem
      simp
      omega

private theorem negSum_nonneg (x : Series) : 0 ≤ negSum x := by
  unfold negSum
  induction x with
  | nil => simp
  | cons a xs ih =>
      simp only [List.map_cons, List.sum_cons]
      omega

private theorem mem_windows_iff {z : Series} {w : Series} :
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

private theorem mem_windowSums_iff {z : Series} {s : Int} :
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

private theorem window_le_measure {z : Series} {s : Int}
    (hs : s ∈ (windows z).map List.sum) : s ≤ BestFrame.measure z := by
  unfold BestFrame.measure
  exact le_max0 hs

private theorem measure_tail_le_cons (a : Int) (l : Series) :
    BestFrame.measure l ≤ BestFrame.measure (a :: l) := by
  unfold BestFrame.measure
  apply max0_le
  · exact max0_nonneg _
  · intro s hs
    apply le_max0
    rcases mem_windowSums_iff.mp hs with ⟨i, hi, j, hj, hs_eq⟩
    apply mem_windowSums_iff.mpr
    have hlen : (a :: l).length = l.length + 1 := by simp
    refine ⟨i + 1, ?_, j, ?_, ?_⟩
    · rw [hlen]
      omega
    · rw [hlen]
      omega
    · simpa using hs_eq

private theorem prefix_window_le_measure (a : Int) (l : Series) (j : Nat) (hj : j ≤ l.length) :
    a + (l.take j).sum ≤ BestFrame.measure (a :: l) := by
  apply window_le_measure
  apply mem_windowSums_iff.mpr
  refine ⟨0, by simp, j, ?_, ?_⟩
  · simp
    omega
  · simp

private theorem measure_cons_le_bound (a : Int) (l : Series) (B : Int)
    (hB : 0 ≤ B) (htail : BestFrame.measure l ≤ B)
    (hpref : ∀ j, j ≤ l.length → a + (l.take j).sum ≤ B) :
    BestFrame.measure (a :: l) ≤ B := by
  unfold BestFrame.measure
  apply max0_le
  · exact hB
  · intro s hs
    rcases mem_windowSums_iff.mp hs with ⟨i, hi, j, hj, hs_eq⟩
    cases i with
    | zero =>
        have hlen : (a :: l).length = l.length + 1 := by simp
        rw [hlen] at hj
        have hjle : j ≤ l.length := by omega
        have hp := hpref j hjle
        rw [← hs_eq]
        simpa using hp
    | succ i' =>
        have hlen : (a :: l).length = l.length + 1 := by simp
        rw [hlen] at hi hj
        have hi' : i' < l.length := by omega
        have hj' : j < l.length - i' := by omega
        have hwin : ((l.drop i').take (j + 1)).sum ≤ BestFrame.measure l := by
          apply window_le_measure
          exact mem_windowSums_iff.mpr ⟨i', hi', j, hj', rfl⟩
        have hdrop : (a :: l).drop (Nat.succ i') = l.drop i' := by simp
        rw [hdrop] at hs_eq
        rw [← hs_eq]
        omega

/-! Every prefix sum of the filtered list is bounded by some prefix sum of the
    original list plus the total magnitude of the dropped negative entries. -/
private theorem filter_prefix_le (xs : Series) (keep : Int → Bool) :
    ∀ k, k ≤ (xs.filter keep).length →
      ∃ j, j ≤ xs.length ∧
        ((xs.filter keep).take k).sum ≤
          (xs.take j).sum + negSum (xs.filter (fun v => !keep v)) := by
  induction xs with
  | nil =>
      intro k hk
      simp at hk
      subst k
      refine ⟨0, by simp, ?_⟩
      simp [negSum]
  | cons a tl ih =>
      intro k hk
      by_cases hka : keep a
      · by_cases hk0 : k = 0
        · subst k
          refine ⟨0, by simp, ?_⟩
          simpa using (negSum_nonneg ((a :: tl).filter (fun v => !keep v)))
        · have hkpos : 0 < k := by omega
          have hk_le : k - 1 ≤ (tl.filter keep).length := by
            simpa [hka] using hk
          obtain ⟨j, hjle, hle⟩ := ih (k - 1) hk_le
          refine ⟨j + 1, by simp [hjle], ?_⟩
          have hkdec : k = (k - 1) + 1 := by omega
          rw [hkdec]
          simp [hka, negSum] at hle ⊢
          omega
      · have hk_le : k ≤ (tl.filter keep).length := by simpa [hka] using hk
        obtain ⟨j, hjle, hle⟩ := ih k hk_le
        refine ⟨j + 1, by simp [hjle], ?_⟩
        simp [hka, negSum] at hle ⊢
        omega

theorem sound_BestFrame_DeletionFence : BestFrame.DeletionFence.Goal := by
  intro x keep
  change BestFrame.measure (x.filter keep) ≤
    BestFrame.measure x + negSum (x.filter (fun v => !keep v))
  induction x with
  | nil =>
      simp [BestFrame.measure, windows, negSum, max0]
  | cons a xs ih =>
      by_cases hka : keep a
      · simp [hka] at ⊢ ih
        have hB0 : 0 ≤ BestFrame.measure (a :: xs) + negSum (xs.filter (fun v => !keep v)) := by
          have h1 : 0 ≤ BestFrame.measure (a :: xs) := by
            unfold BestFrame.measure
            exact max0_nonneg _
          have h2 : 0 ≤ negSum (xs.filter (fun v => !keep v)) := negSum_nonneg _
          omega
        apply measure_cons_le_bound a (xs.filter keep)
          (BestFrame.measure (a :: xs) + negSum (xs.filter (fun v => !keep v)))
        · exact hB0
        · have hsub := measure_tail_le_cons a xs
          omega
        · intro j hjle
          obtain ⟨t, htle, hle⟩ := filter_prefix_le xs keep j hjle
          have hpref := prefix_window_le_measure a xs t htle
          omega
      · simp [hka, negSum] at ⊢ ih
        have hsub := measure_tail_le_cons a xs
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_DeletionFence
