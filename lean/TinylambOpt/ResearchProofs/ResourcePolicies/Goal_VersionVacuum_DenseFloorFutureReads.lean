import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_VersionVacuum_DenseFloorFutureReads

/-!
Registers the existing checked result for `VersionVacuum.DenseFloorFutureReads.Goal`.

/-- One predecessor suffices across all future snapshot times above the oldest pin. -/

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.VersionVacuum.Goal_DenseFloorFutureReads` and re-exported here so that
`VersionVacuum_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_DenseFloorFutureReads

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum.DenseFloorFutureReads.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.VersionVacuum.Goal_DenseFloorFutureReads.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_DenseFloorFutureReads
