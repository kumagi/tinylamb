import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_TypedLinkEndpointRoleRouting

Moving the target-post semijoin before the source-score lookup preserves the
ordered nullable payloads: a link whose target count is zero expands to an
empty payload, so it can be dropped from the flatMap.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_TypedLinkEndpointRoleRouting

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.TypedLinkEndpointRoleRouting.Goal := by
  intro links scores targets
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.TypedLinkEndpointRoleRouting.Goal]
  induction links with
  | nil => simp
  | cons r rs ih =>
      have ih' :
          List.flatMap (fun r : Nat × Nat =>
              List.flatMap (List.replicate (List.count r.2 targets)) (scores r.1))
              (List.filter (fun r => decide (r.2 ∈ targets)) rs) =
            List.flatMap (fun r : Nat × Nat =>
              List.flatMap (List.replicate (List.count r.2 targets)) (scores r.1)) rs := by
        simpa using ih
      by_cases h : r.2 ∈ targets
      · simp [h, ih']
      · have hc : targets.count r.2 = 0 := List.count_eq_zero_of_not_mem h
        simp [h, hc, ih']

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_TypedLinkEndpointRoleRouting
