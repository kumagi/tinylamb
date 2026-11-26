import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_TypedLinkEndpointRoleRouting

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! Pushing the target-post semijoin ahead of the endpoint score lookup is safe:
    a link with no matching target emits an empty payload, so filtering it out
    does not change the ordered nullable score stream. -/

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.TypedLinkEndpointRoleRouting.Goal := by
  intro links scores targets
  induction links with
  | nil => simp
  | cons r rs ih =>
      have ih' :
          List.flatMap (fun q => List.flatMap (List.replicate (targets.count q.2)) (scores q.1))
            (List.filter (fun q => decide (q.2 ∈ targets)) rs) =
          List.flatMap (fun q => List.flatMap (List.replicate (targets.count q.2)) (scores q.1)) rs := by
        simpa [List.contains_iff_mem] using ih
      rw [List.filter_cons]
      by_cases hmem : r.2 ∈ targets
      · simp [hmem, ih']
      · have hc : targets.count r.2 = 0 := (List.count_eq_zero).mpr hmem
        simp [hmem, hc, ih']
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_TypedLinkEndpointRoleRouting
