import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_QualifiedTitleYearHistogram. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_QualifiedTitleYearHistogram

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# QualifiedTitleYearHistogram

When every credit year lies in `[lo, lo + span)`, one bucket per offset
reconstructs both `COUNT(*)` and `SUM(year)`. Each credit contributes to exactly
one bucket, at offset `year - lo`; summing the per-bucket counts and their year
weights therefore recovers the original count and year sum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem krs_sum_zero_map (span : Nat) : ((List.range span).map (fun _ : Nat => 0)).sum = 0 := by
  induction span with
  | zero => simp
  | succ span ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp [ih]

theorem krs_range_pick (d span v : Nat) (hd : d < span) :
    ((List.range span).map (fun i => if i = d then v else 0)).sum = v := by
  induction span with
  | zero => omega
  | succ span ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      by_cases hlt : d < span
      · have ih' := ih hlt
        have hne : span ≠ d := by omega
        simp [hne, ih']
      · have hdeq : d = span := by omega
        have hzero : ((List.range span).map (fun i => if i = span then v else 0)).sum = 0 := by
          have hm : (List.range span).map (fun i => if i = span then v else 0) =
              (List.range span).map (fun _ : Nat => 0) := by
            apply List.map_congr_left
            intro i hi
            have hi_lt : i < span := by simpa using hi
            simp [show i ≠ span by omega]
          rw [hm]
          exact krs_sum_zero_map span
        simp [hdeq, hzero]

theorem krs_sum_map_add (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun i => f i + g i)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons i l ih => simp [ih]; omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.QualifiedTitleYearHistogram.Goal := by
  intro credits lo span h
  dsimp
  induction credits with
  | nil =>
      apply Prod.ext <;> dsimp only
      · simp only [List.map_map]
        change ((List.range span).map (fun _ : Nat => 0)).sum = 0
        exact krs_sum_zero_map span
      · simp only [List.map_map]
        change ((List.range span).map (fun _ : Nat => 0)).sum = 0
        exact krs_sum_zero_map span
  | cons y ys ih =>
      have hy := h y (by simp)
      have hys : ∀ z ∈ ys, lo ≤ z ∧ z < lo + span := by
        intro z hz
        exact h z (by simp [hz])
      have ih' := ih hys
      have ih_len : ((List.range span).map (fun i => ys.count (lo + i))).sum = ys.length := by
        have hf := congrArg Prod.fst ih'
        dsimp only at hf
        simp only [List.map_map] at hf
        change ((List.range span).map (fun i => ys.count (lo + i))).sum = ys.length at hf
        exact hf
      have ih_sum : ((List.range span).map (fun i => (lo + i) * ys.count (lo + i))).sum = ys.sum := by
        have hf := congrArg Prod.snd ih'
        dsimp only at hf
        simp only [List.map_map] at hf
        change ((List.range span).map (fun i => (lo + i) * ys.count (lo + i))).sum = ys.sum at hf
        exact hf
      let d := y - lo
      have hd : d < span := by dsimp [d]; omega
      have hcount :
          (List.range span).map (fun i => (y :: ys).count (lo + i)) =
            (List.range span).map (fun i => ys.count (lo + i) + (if i = d then 1 else 0)) := by
        apply List.map_congr_left
        intro i hi
        have hi_lt : i < span := by simpa using hi
        by_cases hid : i = d
        · have hyeq : y = lo + i := by dsimp [d] at hid; omega
          simp [List.count_cons, hid, hyeq]
        · have hyne : y ≠ lo + i := by dsimp [d] at hid; omega
          simp [List.count_cons, hid, hyne]
      have hweighted :
          (List.range span).map (fun i => (lo + i) * (y :: ys).count (lo + i)) =
            (List.range span).map (fun i => (lo + i) * ys.count (lo + i) + (if i = d then y else 0)) := by
        apply List.map_congr_left
        intro i hi
        have hi_lt : i < span := by simpa using hi
        by_cases hid : i = d
        · have hyeq : y = lo + i := by dsimp [d] at hid; omega
          simp [List.count_cons, hid, hyeq, Nat.mul_add]
        · have hyne : y ≠ lo + i := by dsimp [d] at hid; omega
          simp [List.count_cons, hid, hyne, Nat.mul_add]
      apply Prod.ext <;> dsimp only
      · simp only [List.map_map]
        change ((List.range span).map (fun i => (y :: ys).count (lo + i))).sum = (y :: ys).length
        rw [hcount, krs_sum_map_add, krs_range_pick d span 1 hd, ih_len]
        simp
      · simp only [List.map_map]
        change ((List.range span).map (fun i => (lo + i) * (y :: ys).count (lo + i))).sum = (y :: ys).sum
        rw [hweighted, krs_sum_map_add, krs_range_pick d span y hd, ih_sum]
        simp [List.sum_cons]
        omega
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_QualifiedTitleYearHistogram
