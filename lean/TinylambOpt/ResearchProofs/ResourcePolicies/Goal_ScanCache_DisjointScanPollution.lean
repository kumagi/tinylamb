import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_ScanCache_DisjointScanPollution

/-!
Registers the existing checked result for `ScanCache.DisjointScanPollution.Goal`.

/-- Charge distinct, more-recent keys plus the target for an exact reuse test after its last touch. -/
def WeightedReuseDistance.Goal : Prop := ∀ bytes cap k between, ¬ k ∈ between →
  (k ∈ run bytes cap [] (k::between) ↔ bytes k + (between.eraseDups.map bytes).sum ≤ cap)

/-- Increasing a byte cap never loses an LRU hit, even with blocks larger than the small cache. -/
def WeightedCacheStackInclusion.Goal : Prop := ∀ bytes a b trace, a ≤ b →
  (run bytes a [] trace).IsPrefix (run bytes b [] trace)

/-- An oversized one-pass block can bypass insertion without losing any hits in the remainder of the trace. -/
def OversizeReadBypass.Goal : Prop := ∀ bytes cap cache k tail, cap < bytes k →
  demandMisses bytes cap cache tail ≤ demandMisses bytes cap (touch bytes cap cache k) tail

/-- Bypass a disjoint one-pass scan before returning to the hot set; compare misses on the subsequent workload. -/

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_DisjointScanPollution` and re-exported here so that
`ScanCache_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_DisjointScanPollution

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.DisjointScanPollution.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_DisjointScanPollution.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_DisjointScanPollution
