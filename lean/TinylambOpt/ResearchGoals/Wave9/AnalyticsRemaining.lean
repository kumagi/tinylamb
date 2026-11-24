import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

namespace TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining

open TinylambOpt.ResearchGoals.AnalyticsWorkloads (Totals zero weighted plus summarize histogram)

/-! ## Wave 9: Remaining SF=10 Analytics Bottlenecks (C3, C2, H6, L4/L5)

Targets:
- STATS-CEB C3 (66.3ms vs DuckDB 8.2ms, 8.1x): `SelfJoinBridgeBipartiteWeights`
- H2O H6 (21.7ms vs ClickHouse 3.8ms, 5.7x): `MultiKeyInRangeZoneBlockSkip`
- STATS-CEB C2 (42.6ms vs DuckDB 15.8ms, 2.7x): `DualRoleUniqueIdBitsetRoute`
- LDBC-SNB L4/L5 (12.7/11.1ms vs 5.4/4.1ms): `FilteredSemiJoinCountBeforeGroup`
-/

/-- STATS-CEB C3. Single-pass `posts` profile builds both source `(cnt, nn, sum)`
and filtered target multiplicity `type=1`; streaming `post_links` computes exact
`link_type` grouped totals without scanning `posts` twice or materializing joins. -/
abbrev PostRec := Nat × Bool × Option Int -- (id, isType1, score)
abbrev LinkRec := Nat × Nat × Nat         -- (post_id, related_post_id, link_type)

def singleScanBridgeAgg (posts : List PostRec) (links : List LinkRec) (lt : Nat) : Totals :=
  let (src, dst) := posts.foldl (fun (s, d) (id, ok, sc) =>
    let s' := s.insert id (plus (s.getD id zero) (weighted 1 sc))
    let d' := if ok then d.insert id (d.getD id 0 + 1) else d
    (s', d')) (({} : Std.HashMap Nat Totals), ({} : Std.HashMap Nat Nat))
  links.foldl (fun acc (u, v, t) =>
    if t != lt then acc else
      let a := src.getD u zero
      let m := dst.getD v 0
      plus acc ⟨a.cnt * m, a.nn * m, a.sum * m⟩) zero

def refBridgeAgg (posts : List PostRec) (links : List LinkRec) (lt : Nat) : Totals :=
  summarize (links.flatMap fun (u, v, t) =>
    if t != lt then [] else
      (posts.filter fun p => p.1 == u).flatMap fun a =>
        (posts.filter fun p => p.1 == v && p.2.1).map fun _ => a.2.2)

def SelfJoinBridgeBipartiteWeights.Goal : Prop :=
  ∀ (posts : List PostRec) (links : List LinkRec) (lt : Nat),
    singleScanBridgeAgg posts links lt = refBridgeAgg posts links lt

/-- H2O H6. A block whose `id1` `[min1, max1]` contains no element of `S` OR whose
`id2` `[min2, max2]` is disjoint from `[lo, hi]` contributes zero rows to
`WHERE id1 IN S AND id2 BETWEEN lo AND hi`. -/
def blockMayHitInRange (min1 max1 min2 max2 lo hi : Int) (s : List Int) : Bool :=
  s.any (fun x => min1 ≤ x && x ≤ max1) && !(max2 < lo || hi < min2)

def MultiKeyInRangeZoneBlockSkip.Goal : Prop :=
  ∀ (rows : List (Int × Int × Option Int)) (min1 max1 min2 max2 lo hi : Int) (s : List Int),
    (∀ r ∈ rows, min1 ≤ r.1 ∧ r.1 ≤ max1 ∧ min2 ≤ r.2.1 ∧ r.2.1 ≤ max2) →
    blockMayHitInRange min1 max1 min2 max2 lo hi s = false →
    summarize ((rows.filter fun r => s.contains r.1 && lo ≤ r.2.1 && r.2.1 ≤ hi).map fun r => r.2.2) = zero

/-- STATS-CEB C2. Build two boolean predicates `(u_ok, u2_ok)` in one scan of unique
`users`, build `post -> owner` only for `u_ok` owners, and count directly per owner
while streaming `comments` filtered by `u2_ok`. -/
structure UserRow where
  id : Nat
  yrOk : Bool
  repOk : Bool

def dualBitsetCount (users : List UserRow) (posts : List (Nat × Nat))
    (comments : List (Nat × Nat)) (ownerId : Nat) : Nat :=
  let (uOk, u2Ok) := users.foldl (fun (a, b) u =>
    (if u.yrOk then a.insert u.id else a, if u.repOk then b.insert u.id else b))
    (({} : Std.HashSet Nat), ({} : Std.HashSet Nat))
  let postOwner : Std.HashMap Nat Nat :=
    posts.foldl (fun m (pid, oid) => if uOk.contains oid then m.insert pid oid else m) {}
  comments.countP fun (pid, uid) => u2Ok.contains uid && postOwner[pid]? == some ownerId

def refDualUserCount (users : List UserRow) (posts : List (Nat × Nat))
    (comments : List (Nat × Nat)) (ownerId : Nat) : Nat :=
  (comments.flatMap fun (pid, uid) =>
    (posts.filter fun p => p.1 == pid).flatMap fun p =>
      (users.filter fun u => u.id == p.2 && u.yrOk && u.id == ownerId).flatMap fun _ =>
        users.filter fun u2 => u2.id == uid && u2.repOk).length

def DualRoleUniqueIdBitsetRoute.Goal : Prop :=
  ∀ (users : List UserRow) (posts comments : List (Nat × Nat)) (ownerId : Nat),
    (users.map UserRow.id).Nodup →
    (posts.map Prod.fst).Nodup →
    dualBitsetCount users posts comments ownerId = refDualUserCount users posts comments ownerId

/-- LDBC-SNB L4 / L5. Pre-aggregating edge out-degrees (`src -> deg`) before joining
the entity table preserves exact weighted `(cnt, nn, sum)` per entity group without
materializing edges. -/
def degreeWeightedAgg (entities : List (Nat × Nat × Option Int))
    (edges : List (Nat × Nat)) (groupKey : Nat) : Totals :=
  let deg := histogram (edges.map Prod.fst)
  entities.foldl (fun acc (id, g, v) =>
    if g == groupKey then plus acc (weighted (deg.getD id 0) v) else acc) zero

def refEdgeJoinAgg (entities : List (Nat × Nat × Option Int))
    (edges : List (Nat × Nat)) (groupKey : Nat) : Totals :=
  summarize (entities.flatMap fun (id, g, v) =>
    if g != groupKey then [] else (edges.filter fun e => e.1 == id).map fun _ => v)

def FilteredSemiJoinCountBeforeGroup.Goal : Prop :=
  ∀ (entities : List (Nat × Nat × Option Int)) (edges : List (Nat × Nat)) (groupKey : Nat),
    degreeWeightedAgg entities edges groupKey = refEdgeJoinAgg entities edges groupKey

#guard singleScanBridgeAgg [(1, true, some 5), (2, true, none)] [(1, 2, 3)] 3 ==
  refBridgeAgg [(1, true, some 5), (2, true, none)] [(1, 2, 3)] 3
#guard blockMayHitInRange 10 20 50 60 10 40 [1, 2, 3] == false
#guard dualBitsetCount [⟨1, true, false⟩, ⟨2, false, true⟩] [(10, 1)] [(10, 2), (10, 1)] 1 == 1
#guard degreeWeightedAgg [(1, 7, some 30), (2, 7, none)] [(1, 9), (1, 8), (2, 9)] 7 ==
  refEdgeJoinAgg [(1, 7, some 30), (2, 7, none)] [(1, 9), (1, 8), (2, 9)] 7

end TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining
