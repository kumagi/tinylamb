import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# VoteWeightedCommentAggregates

Weighting comments by the corresponding post's vote count preserves the join count and
nullable sum. The aggregate can avoid generating comment-vote pairs.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.VoteWeightedCommentAggregates

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_flatMap_replicate)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.VoteWeightedCommentAggregates.Goal := by
  intro scores votes
  dsimp only
  apply Prod.ext
  · simp [List.length_flatMap, List.map_const', List.sum_replicate_nat]
  · simpa [beq_iff_eq] using nsum_flatMap_replicate votes scores

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.VoteWeightedCommentAggregates

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.VoteWeightedCommentAggregates.sound

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
