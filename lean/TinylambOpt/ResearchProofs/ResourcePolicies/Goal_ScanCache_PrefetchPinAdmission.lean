import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_ScanCache_PrefetchPinAdmission

/-!
Registers the existing checked result for `ScanCache.PrefetchPinAdmission.Goal`.

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
def DisjointScanPollution.Goal : Prop := ∀ bytes cap warm scan probes,
  (∀ k ∈ scan, ¬ k ∈ probes) →
  demandMisses bytes cap (run bytes cap [] warm) probes ≤
    demandMisses bytes cap (run bytes cap (run bytes cap [] warm) scan) probes

/-- Reusing only the number of intervening keys would allow a constant-time hit prediction for heterogeneous block sizes. -/
def CountReuseForByteCache.Goal : Prop := ∀ bytes cap k between, ¬ k ∈ between → bytes k > 0 →
  bytes k * (between.eraseDups.length+1) ≤ cap → k ∈ run bytes cap [] (k::between)

/-- A cap reduction can trim the current prefix; a reduced cache from startup has the same resident ids. -/
def OnlineCacheShrink.Goal : Prop := ∀ bytes a b trace, a ≤ b →
  fit bytes a (run bytes b [] trace) = run bytes a [] trace

/-- The next probe after speculative prefetch must still hit if the prefetch and the entire old prefix fit together. -/

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_PrefetchPinAdmission` and re-exported here so that
`ScanCache_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_PrefetchPinAdmission

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache.PrefetchPinAdmission.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ScanCache.Goal_PrefetchPinAdmission.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_PrefetchPinAdmission
