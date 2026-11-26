import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_DiscountBlockRevenueDotProduct

Registers the proof of the DiscountBlockRevenueDotProduct goal in KeyRoutedJoinStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_DiscountBlockRevenueDotProduct
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.DiscountBlockRevenueDotProduct

theorem sound : Goal := by
  intro lanes base
  simp only [Goal, total]
  induction lanes with
  | nil => simp
  | cons x xs ih =>
      rcases x with ⟨r, d⟩
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      simp [Int.add_mul, Int.mul_add]
      omega

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_DiscountBlockRevenueDotProduct
