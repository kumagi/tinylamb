import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# SkipVoteReadsWithoutComments

Votes from a qualifying post with no matching comments cannot contribute. The executor
can check comment existence before reading the vote list.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.SkipVoteReadsWithoutComments

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.SkipVoteReadsWithoutComments.Goal := by
  intro posts comments votes
  dsimp only
  induction posts with
  | nil => simp
  | cons k t ih =>
      by_cases hc : k ∈ comments
      · simp [hc, ih]
      · have hcnt : comments.count k = 0 := (List.count_eq_zero).2 hc
        simp [hc, hcnt, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.SkipVoteReadsWithoutComments

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.SkipVoteReadsWithoutComments.sound
