import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# DisjointShippingDelayBuckets

A shipping delay maps to the same modeled indicator counts through a single bucket
index. The executor can replace several CASE evaluations with one classification.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.DisjointShippingDelayBuckets
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.DisjointShippingDelayBuckets.Goal := by
  intro d
  dsimp only
  by_cases h1 : 30 < d <;> by_cases h2 : 60 < d <;>
  by_cases h3 : 90 < d <;> by_cases h4 : 120 < d <;>
  simp [bucket, h1, h2, h3, h4] <;> omega

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.DisjointShippingDelayBuckets

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.DisjointShippingDelayBuckets.sound

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
