import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-! Memory and work admission for a duplicate-preserving equijoin. Count actual
matching pairs, exclude NULL keys, and allow sharding of an individual build key.
Row identities in a posting list are distinct positions even when payloads match. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
open TinylambOpt.ResearchGoals.QueryRewrites

abbrev Keys := List (Option Nat)
def pairs (a b : Keys) := a.flatMap (fun k => b.filter (hit k))
def hist (ks : Keys) := frequency (ks.filterMap id)
def product (a b : Keys) := ((hist a).map (fun (k,n) => n*b.count (some k))).sum

/-- A nullable-key frequency synopsis predicts the exact output cardinality, including duplicate probe rows. -/
def NullAwareFanoutReservation.Goal : Prop := ∀ a b, (pairs a b).length = product a b

/-- If the build and probe sides have common heavy hitters, separately measured maximum frequencies can be paired for a tight estimate. -/
def IndependentPeakFanoutEstimate.Goal : Prop := ∀ a b,
  let maxA := ((hist a).map Prod.snd).max?.getD 0
  let maxB := ((hist b).map Prod.snd).max?.getD 0
  0 < product a b → maxA*maxB ≤ product a b

/-- Max-frequency bounds on both sides cap an output reservation even when the matched key sets are unknown. -/
def TwoSidedFanoutEnvelope.Goal : Prop := ∀ a b,
  product a b ≤ min ((a.filterMap id).length*(((hist b).map Prod.snd).max?.getD 0))
    ((b.filterMap id).length*(((hist a).map Prod.snd).max?.getD 0))

/-- Bucket-level products overestimate only pairs of unequal keys that collide in the routing function. -/
def PartitionSynopsisCollisionTax.Goal : Prop := ∀ (a b : Keys) (route : Nat → Nat),
  product (a.map (Option.map route)) (b.map (Option.map route)) =
    product a b + ((a.filterMap id).map (fun k =>
      ((b.filterMap id).filter (fun j => k != j && route k == route j)).length)).sum

/-- A hot key split by build-row ordinal can reuse the probe once per shard without duplicating output pairs. -/
def StripedHotKeyCoverage.Goal : Prop := ∀ (rows probes : Nat) stripes, 0 < stripes →
  ((List.range stripes).flatMap (fun s =>
    ((List.range rows).filter (fun i => i%stripes == s)).flatMap (fun i =>
      (List.range probes).map (fun j => (i,j))))).Perm
        ((List.range rows).flatMap (fun i => (List.range probes).map (fun j => (i,j))))

/-- Salting both sides by local ordinal avoids probe replication while preserving every duplicate match. -/
def SymmetricHotKeySalting.Goal : Prop := ∀ rows probes stripes, 0 < stripes →
  ((List.range rows).map (fun i => ((List.range probes).filter (fun j => i%stripes == j%stripes)).length)).sum =
    rows*probes

/-- Exact unmatched probe count permits sizing an outer join's output before gathering any nullable build payload. -/
def LeftOuterUnmatchedReserve.Goal : Prop := ∀ a b,
  product a b + (a.filter (fun k => !(b.any (hit k)))).length =
    (a.map (fun k => max 1 (b.filter (hit k)).length)).sum

/-- Unique build keys bound fanout on the inner path even if multiple NULLs bypass a uniqueness constraint. -/
def NullableUniqueBuildFanout.Goal : Prop := ∀ a b, (b.filterMap id).Nodup →
  product a b ≤ (a.filterMap id).length

/-- Local work estimated by bucket products is additive after eliminating keys routed to different buckets. -/
def DisjointBuildRoutingWork.Goal : Prop := ∀ a b (route : Nat → Bool),
  let side := fun (flag : Bool) (ks : Keys) => ks.filter (fun k => k.any (fun x => route x == flag))
  product (side true a) (side true b) + product (side false a) (side false b) = product a b

#guard product [none,some 2,some 2] [some 2,none,some 2] == 4

end TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
