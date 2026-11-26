import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ShiftedStoreWeekIndexWithBoundaryHalo

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# ShiftedStoreWeekIndexWithBoundaryHalo

Restricting right-side week keys to the shifted interval of the left partition
preserves every join multiplicity. For a left week inside the bounded interval,
the shifted key lies inside the halo, so filtering the right input cannot change
its count. The executor may build only the halo index, including inclusive
boundary weeks.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

/-! Counting a value after a boolean filter is unchanged when the value itself
satisfies the filter. -/
theorem count_filter_eq_of_mem {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (p : α → Bool) (v : α) (hp : p v = true) :
    (l.filter p).count v = l.count v := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      by_cases hpa : p a = true
      · by_cases hav : a = v
        · subst a
          simp [hpa, hp, ih]
        · simp [hpa, hav, ih]
      · by_cases hav : a = v
        · subst a
          simp [hp, hpa]
        · simp [hpa, hav, ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.ShiftedStoreWeekIndexWithBoundaryHalo.Goal := by
  intro left right lo hi hleft
  dsimp only
  apply List.map_congr_left
  intro k hk
  have hk' := hleft k hk
  have hlo : lo + 52 ≤ k + 52 := by omega
  have hhi : k + 52 ≤ hi + 52 := by omega
  have hp : (decide (lo + 52 ≤ k + 52) && decide (k + 52 ≤ hi + 52)) = true := by
    simp [hlo, hhi]
  exact count_filter_eq_of_mem right
    (fun w => decide (lo + 52 ≤ w) && decide (w ≤ hi + 52)) (k + 52) hp
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ShiftedStoreWeekIndexWithBoundaryHalo
