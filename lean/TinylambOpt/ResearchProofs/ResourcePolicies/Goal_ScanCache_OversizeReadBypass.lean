import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_ScanCache_OversizeReadBypass

/-!
Registers the existing checked result for `ScanCache.OversizeReadBypass.Goal`.

/-- Charge distinct, more-recent keys plus the target for an exact reuse test after its last touch. -/
def WeightedReuseDistance.Goal : Prop := ∀ bytes cap k between, ¬ k ∈ between →
  (k ∈ run bytes cap [] (k::between) ↔ bytes k + (between.eraseDups.map bytes).sum ≤ cap)

/-- Increasing a byte cap never loses an LRU hit, even with blocks larger than the small cache. -/
def WeightedCacheStackInclusion.Goal : Prop := ∀ bytes a b trace, a ≤ b →
  (run bytes a [] trace).IsPrefix (run bytes b [] trace)

/-- An oversized one-pass block can bypass insertion without losing any hits in the remainder of the trace. -/

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_OversizeReadBypass` and re-exported here so that
`ScanCache_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_OversizeReadBypass

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.OversizeReadBypass.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_OversizeReadBypass.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_OversizeReadBypass
