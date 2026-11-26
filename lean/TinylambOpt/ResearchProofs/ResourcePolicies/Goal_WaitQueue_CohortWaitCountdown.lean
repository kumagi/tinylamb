import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_WaitQueue_CohortWaitCountdown

/-!
Registers the existing checked result for `WaitQueue.CohortWaitCountdown.Goal`.

/-- Count writer turns and reader runs separately to bound head-of-line delay in scheduler turns. -/

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_CohortWaitCountdown` and re-exported here so that
`WaitQueue_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_CohortWaitCountdown

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.CohortWaitCountdown.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_CohortWaitCountdown.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_CohortWaitCountdown
