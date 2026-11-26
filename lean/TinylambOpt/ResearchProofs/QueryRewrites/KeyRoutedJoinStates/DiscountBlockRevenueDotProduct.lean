import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# DiscountBlockRevenueDotProduct

For qualifying lanes, frame-of-reference prices allow one base multiplication
and a dot product of residuals with discounts.
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.DiscountBlockRevenueDotProduct
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.DiscountBlockRevenueDotProduct.Goal := by
  intro lanes base
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.DiscountBlockRevenueDotProduct.Goal, total]
  induction lanes with
  | nil => simp
  | cons x xs ih =>
      rcases x with ⟨r, d⟩
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      simp [Int.add_mul, Int.mul_add]
      omega

end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.DiscountBlockRevenueDotProduct

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.DiscountBlockRevenueDotProduct.sound
