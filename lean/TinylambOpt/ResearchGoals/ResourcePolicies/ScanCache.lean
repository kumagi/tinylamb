import Std

/-! Read-through cache with variant-sized immutable blocks and scan bypass. MRU
is first. A demand reference moves its key to the head; the byte ceiling keeps
a contiguous recency prefix. A separate key includes the file generation. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

def fit (bytes : Nat → Nat) : Nat → List Nat → List Nat
  | _, [] => []
  | cap, i::is => if bytes i ≤ cap then i::fit bytes (cap-bytes i) is else []
def touch (bytes : Nat → Nat) (cap : Nat) (cache : List Nat) (k : Nat) :=
  fit bytes cap (k :: cache.erase k)
def run (bytes : Nat → Nat) (cap : Nat) (cache trace : List Nat) :=
  trace.foldl (touch bytes cap) cache
def demandMisses (bytes : Nat → Nat) (cap : Nat) : List Nat → List Nat → Nat
  | _, [] => 0
  | cache, k::ks => (if cache.contains k then 0 else 1) + demandMisses bytes cap (touch bytes cap cache k) ks

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
def PrefetchPinAdmission.Goal : Prop := ∀ bytes cap cache incoming k, k ∈ cache → cache.Nodup →
  (cache.map bytes).sum + (incoming.map bytes).sum ≤ cap → k ∈ run bytes cap cache incoming

structure Request where
  file : Nat
  generation : Nat
  block : Nat
  deriving DecidableEq, Repr

/-- Duplicate asynchronous block fetches may share a response exactly when all parts of the immutable key match. -/
def CoalescedFetchScatter.Goal : Prop := ∀ (rs : List Request) (readBlock : Request → List Nat),
  let batch := rs.eraseDups
  rs.map (fun r => (batch.map readBlock)[batch.idxOf r]?) = (rs.map readBlock).map some

/-- A speculative response addressed only by file and block can fill a replacement generation's cache slot. -/
def GenerationBlindPrefetchFill.Goal : Prop := ∀ (readBlock : Request → List Nat) f b g h,
  g < h → readBlock ⟨f,g,b⟩ = readBlock ⟨f,h,b⟩

/-- Fixed shard quotas can reproduce a pooled cache on each trace if their total equals its capacity. -/
def FixedShardCacheMissParity.Goal : Prop := ∀ trace cap,
  let left := trace.filter (fun k => k%2 == 0)
  let right := trace.filter (fun k => k%2 == 1)
  demandMisses (fun _ => 1) cap [] left + demandMisses (fun _ => 1) cap [] right =
    demandMisses (fun _ => 1) (2*cap) [] trace

#guard run (fun _ => 1) 2 [] [0,1,0,2] == [2,0]

end TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache
