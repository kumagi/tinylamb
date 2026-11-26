import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_DiscountBlockRevenueDotProduct. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_DiscountBlockRevenueDotProduct

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# DiscountBlockRevenueDotProduct

Revenue over qualifying lanes is `SUM ((base + r) * d)`. Expanding the
multiplication and distributing the exact integer sum gives one base scaling
plus a dot product over residuals, so the executor may reuse a single base
multiplier. No assumption about lane order or duplicate lanes is needed.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites (total)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.DiscountBlockRevenueDotProduct.Goal := by
  intro lanes base
  dsimp [total]
  induction lanes with
  | nil => simp
  | cons lane lanes ih =>
      rcases lane with ⟨r, d⟩
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [Int.add_mul, Int.mul_add]
      omega
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_DiscountBlockRevenueDotProduct
