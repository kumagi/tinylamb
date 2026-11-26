import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# ForumReplyFrequencyJoinCount

Grouping eligible 2012 replies by forum before joining preserves the modeled forum
result. The executor can send one subtotal per forum to the join.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.ForumReplyFrequencyJoinCount

theorem sum_replicate_flatten (n : Nat) (xs : List Int) : (List.replicate n xs).flatten.sum = (n : Int) * xs.sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.flatten_cons, List.sum_append, ih]
      rw [Int.natCast_add, Int.add_mul]
      simp [Int.add_comm]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.ForumReplyFrequencyJoinCount.Goal := by
  intro scores forumCopies
  dsimp only
  induction forumCopies with
  | zero => simp
  | succ n ih =>
      have hsum := sum_replicate_flatten (n + 1) (List.filterMap id scores)
      simp [ih, hsum]

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.ForumReplyFrequencyJoinCount

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.ForumReplyFrequencyJoinCount.sound

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
