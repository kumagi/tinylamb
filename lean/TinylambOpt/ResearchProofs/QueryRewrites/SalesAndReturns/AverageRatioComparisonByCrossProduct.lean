import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# AverageRatioComparisonByCrossProduct

Under the stated positivity and exactness conditions, the comparison with the state
average can be expressed using cross-products.

The filter can use the state sum and non-NULL count directly instead of dividing to
obtain the threshold for each customer.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.AverageRatioComparisonByCrossProduct
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.AverageRatioComparisonByCrossProduct.Goal := by
  intro stateTotals v
  dsimp only
  intro hn hdiv
  let xs : List Int := stateTotals.filterMap id
  let n : Int := xs.length
  let d : Int := 5 * n
  let S : Int := 6 * xs.sum
  have hd : 0 < d := by
    dsimp [d, n, xs]
    exact Int.mul_pos (by decide) hn
  have h1 : (S / d) * d = S := by
    dsimp [S, d, n, xs]
    exact Int.ediv_mul_cancel hdiv
  constructor
  · intro h
    have hqd : (S / d) * d < v * d := Int.mul_lt_mul_of_pos_right h hd
    rw [h1] at hqd
    rw [show v * d = d * v by ac_rfl] at hqd
    exact hqd
  · intro h
    have hv : S < d * v := h
    rw [← h1] at hv
    rw [show d * v = v * d by ac_rfl] at hv
    exact (Int.mul_lt_mul_right hd).mp hv
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.AverageRatioComparisonByCrossProduct
