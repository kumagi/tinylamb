import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# ShiftedStoreWeekIndexWithBoundaryHalo

A bounded left week partition needs right-side sales only from the
corresponding shifted interval.
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ShiftedStoreWeekIndexWithBoundaryHalo

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.ShiftedStoreWeekIndexWithBoundaryHalo.Goal := by
  intro left right lo hi hlohi
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.ShiftedStoreWeekIndexWithBoundaryHalo.Goal]
  apply List.map_congr_left
  intro k hk
  have hk' := hlohi k hk
  -- halo.count (k+52) = right.count (k+52)
  -- elements in right but not in halo are outside [lo+52, hi+52], so != k+52
  have hcount : (right.filter (fun w => lo + 52 ≤ w && w ≤ hi + 52)).count (k + 52) = right.count (k + 52) := by
    have hsub : ∀ x ∈ right, x = k + 52 → (lo + 52 ≤ x && x ≤ hi + 52) = true := by
      intro x _ hx; subst hx; simp; omega
    -- count in filter equals count when all matching elements pass filter
    have h1 : (right.filter (fun w => lo + 52 ≤ w && w ≤ hi + 52)).count (k + 52) =
              (right.filter (fun w => (lo + 52 ≤ w && w ≤ hi + 52) && (w == k + 52))).length := by
      rw [List.count_eq_length_filter]
      congr 1
      rw [List.filter_filter]
      apply List.filter_congr
      intro x _
      simp [Bool.and_comm]
    have h2 : right.count (k + 52) = (right.filter (fun w => w == k + 52)).length := by
      rw [List.count_eq_length_filter]
    rw [h1, h2]
    congr 1
    apply List.filter_congr
    intro x hx
    by_cases heq : (x == k + 52) = true
    · have hxeq : x = k + 52 := by simpa using heq
      simp [heq, hsub x hx hxeq]
    · simp [heq]
  exact hcount

end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ShiftedStoreWeekIndexWithBoundaryHalo

