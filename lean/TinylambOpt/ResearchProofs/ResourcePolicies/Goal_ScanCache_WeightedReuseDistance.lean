import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_ScanCache_WeightedReuseDistance

/-!
Registers the existing checked result for `ScanCache.WeightedReuseDistance.Goal`.

/-- Charge distinct, more-recent keys plus the target for an exact reuse test after its last touch. -/

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_WeightedReuseDistance` and re-exported here so that
`ScanCache_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_WeightedReuseDistance

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.WeightedReuseDistance.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_WeightedReuseDistance.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_WeightedReuseDistance
