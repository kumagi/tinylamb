import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

/-!
These models connect SQL operators to their reference rows, order, or aggregate results.
-/

namespace TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts
open AnalyticsWorkloads

abbrev Path := List (Option Nat)
abbrev Fact := Path × Option Int

/-- At one ROLLUP depth, emit a group only when its prefix changes. Depth is a
separate grouping id, so a literal NULL key and a rolled-away key cannot collide. -/
def prefixRuns (depth : Nat) (rs : List Fact) : List (Path × Totals) :=
  let runs := rs.foldl (fun acc (path,v) =>
    let k := path.take depth
    match acc with
    | [] => [(k,weighted 1 v)]
    | (last,a)::done_ =>
      if last == k then (k,plus a (weighted 1 v))::done_ else (k,weighted 1 v)::acc) []
  if rs.isEmpty && depth == 0 then [([],zero)] else runs.reverse

def OrderedRollup.Goal : Prop := ∀ (rs : List Fact) depth k,
  rs.Pairwise (fun a b => compare (a.1.take depth) (b.1.take depth) != .gt) →
  let out := prefixRuns depth rs
  let vs := (rs.filter (fun r => r.1.take depth == k)).map Prod.snd
  out.lookup k = (if !vs.isEmpty || (depth == 0 && k.isEmpty) then some (summarize vs) else none) ∧
    (out.map Prod.fst).Nodup

/-- Q22: prune an unfinished AVG group from an ascending top-k once its best
possible completion is strictly above the cutoff. Existing group must have a
non-NULL measure; future values have a lower bound and a bound on non-NULL count. -/
def AverageFence.Goal : Prop := ∀ (seen tail : List Int) (floor numerator : Int) (denom capacity : Nat),
  0 < denom →
  seen ≠ [] → tail.length ≤ capacity → (∀ x ∈ tail, floor ≤ x) →
  numerator * (seen.length : Int) < (denom:Int)*seen.sum →
  numerator * ((seen.length+capacity : Nat):Int) < (denom:Int)*(seen.sum + floor*(capacity:Int)) →
  numerator * (((seen++tail).length):Int) < (denom:Int)*(seen++tail).sum

/-- Compute changed aggregate keys at a boundary as only those levels strictly
below the common prefix. Sharing the first fields does not imply sharing later fields. -/
def lcp : Path → Path → Nat
  | x::xs,y::ys => if x == y then 1 + lcp xs ys else 0
  | _,_ => 0
def SuffixFlush.Goal : Prop := ∀ (a b : Path), a.length = b.length →
  ((List.range (a.length+1)).filter (fun d => a.take d != b.take d)) =
    (List.range (a.length - lcp a b)).map (fun i => lcp a b + i + 1)

/-- Q72: sort inventory quantities once per (item,week,warehouse). A strict
lower-bound search yields the complete fanout of a sale, including ties on the
inventory side and excluding ties with the sale quantity. -/
def lowerRank (a : Array Int) (q : Int) (lo hi : Nat) : Nat :=
  if h : lo < hi then
    let mid := (lo+hi)/2
    if a[mid]?.any (· < q) then lowerRank a q (mid+1) hi else lowerRank a q lo mid
  else lo
termination_by hi-lo
decreasing_by all_goals simp_wf; omega

def ThresholdFanout.Goal : Prop := ∀ (inv : List (Option Int)) (sales : List (Option Int × Nat)),
  let a := (inv.filterMap id).mergeSort.toArray
  (sales.map (fun (v,w) => w * v.elim 0 (fun q => lowerRank a q 0 a.size))).sum =
    (sales.map (fun (v,w) => w * inv.countP (fun u => u.any (fun q => v.any (q < ·))))).sum

/-- Replace both LEFT JOINs in Q72 by two independent weight lookups. An empty
promotion side produces one no-promo row. The return side has fanout max(1,n). -/
def PromoReturnCounts.Goal : Prop := ∀ (promos : List (Option Nat)) (returns : List Nat),
  let rows := (if promos.isEmpty then [none] else promos).flatMap (fun p =>
    (if returns.isEmpty then [none] else returns.map some).map (fun _ => p))
  let w := max 1 returns.length
  (rows.countP Option.isNone,rows.countP Option.isSome,rows.length) =
    ((if promos.isEmpty then 1 else promos.countP Option.isNone)*w,
      promos.countP Option.isSome*w, max 1 promos.length*w)

/-- Q38: use the smallest projected channel as a candidate set. Stop carrying
keys after a missing channel. Bound membership probes into the last channel. -/
def intersectProbe (seed middle last : List Nat) :=
  seed.eraseDups.foldl (fun (found,calls) k =>
    if middle.contains k then (found+(last.contains k).toNat,calls+1) else (found,calls)) (0,0)
def ShrinkingIntersect.Goal : Prop := ∀ a b c, intersectProbe a b c =
  (((c.filter (fun k => a.contains k && b.contains k)).eraseDups).length,
   (a.eraseDups.filter b.contains).length)

/-- Q87: destructively remove each matched store key when an exclusion arrives,
with no storage for unmatched catalog/web values. NULL components are encoded in
the projected key before this kernel. -/
def ExclusionErase.Goal : Prop := ∀ (store catalog web : List (Option Nat × Option Nat)),
  let active := store.foldl (fun (s : Std.HashSet _) k => s.insert k) {}
  let rest := (catalog ++ web).foldl (fun s k => s.erase k) active
  rest.size = (store.eraseDups.filter (fun k => !catalog.contains k && !web.contains k)).length ∧
    rest.size ≤ active.size

/-- Q75: UNION DISTINCT must compare the entire projected row, including both
net measures. Per-group pair deduplication enables incremental SUM updates. -/
abbrev Net := Option Int × Option Int
def distinctNet (rows : List (Nat × Net)) (group : Nat) :=
  rows.foldl (fun (seen,qty,amt) (g,v) =>
    if g != group || seen.contains v then (seen,qty,amt)
    else (seen.insert v,plus qty (weighted 1 v.1),plus amt (weighted 1 v.2)))
    (({} : Std.HashSet Net),zero,zero)
def NetPairSet.Goal : Prop := ∀ (channels : List (List (Nat × Net))) g,
  let (_,qty,amt) := distinctNet channels.flatten g
  let vs := (channels.flatten.eraseDups.filter (·.1 == g)).map Prod.snd
  (qty,amt) = (summarize (vs.map Prod.fst),summarize (vs.map Prod.snd))

#guard (prefixRuns 0 []).lookup [] == some zero && (prefixRuns 2 []).isEmpty
#guard lowerRank #[1,3,3,9] 3 0 4 == 1 && lowerRank #[] 0 0 0 == 0
#guard lcp [some 1,none,some 3] [some 1,none,none] == 2
#guard intersectProbe [1,1,2] [1] [2,1,1] == (1,1)
#guard (distinctNet [(1,(some 2,none)),(1,(some 2,some 0)),(1,(some 2,none))] 1).2.1 == ⟨2,2,4⟩

end TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts
