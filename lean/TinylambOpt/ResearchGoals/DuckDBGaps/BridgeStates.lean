import TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o
import TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining

namespace TinylambOpt.ResearchGoals.DuckDBGaps.BridgeStates
open AnalyticsWorkloads
open BilateralEagerAggJoinPushdown (LRow RRow refGroup)
open Wave9.AnalyticsRemaining (PostRec LinkRec refBridgeAgg refDualUserCount UserRow)

/-- Two H5 join keys with identical right COUNT/validity/SUM triples act identically
on both projected sums. Accumulate the left rows by that triple, even across keys. -/
def RightStateClasses.Goal : Prop := ∀ (ls : List LRow) (rs : List RRow) g,
  let build := rs.foldl (fun (m : Std.HashMap Int Totals) r => match r.k with
    | none => m | some k => m.insert k (plus (m.getD k zero) (weighted 1 r.v))) {}
  let cells := (ls.filter (·.g == g)).foldl (fun (m : Std.HashMap (Nat × Nat × Int) Totals) l => Id.run do
    let some k := l.k | return m
    let some r := build[k]? | return m
    let sig := (r.cnt,r.nn,r.sum)
    return m.insert sig (plus (m.getD sig zero) (weighted 1 l.v))) {}
  let grouped := cells.toList.foldl (fun groups ((cnt,nn,s),l) =>
    H2o.contribute groups g l ⟨cnt,nn,s⟩) {}
  H2o.finish grouped g = refGroup ls rs g

/-- An out-of-core H5 join can switch strategy independently in each radix shard.
Summing the two accumulator states is required when several shards emit the same group. -/
def ShardedHybrid.Goal : Prop := ∀ (ls : List LRow) (rs : List RRow) g (route : Int → Nat)
    (hot : Nat → Int → Bool) workers, (∀ k, route k < workers) →
  let states := (List.range workers).map (fun j =>
    let a := ls.filter (fun l => l.k.any (fun k => route k == j))
    let b := rs.filter (fun r => r.k.any (fun k => route k == j))
    (H2o.hybridGroups a b (hot j)).getD g (zero,zero))
  let (a,b) := states.foldl (fun (u,v) (s,t) => (plus u s,plus v t)) (zero,zero)
  H2o.finish (({} : H2o.Groups).insert g (a,b)) g = refGroup ls rs g

/-- C3: compress repeated edges by endpoint pair within a link type. Project target
multiplicity back to the source before fetching its score state; self-links are allowed. -/
def SourceWeights.Goal : Prop := ∀ (posts : List PostRec) (es : List LinkRec) t,
  let edges := histogram ((es.filter (fun e => e.2.2 == t)).map (fun e => (e.1,e.2.1)))
  let targets := histogram ((posts.filter (fun p => p.2.1)).map Prod.fst)
  let weight := edges.toList.foldl (fun (h : Std.HashMap Nat Nat) ((u,v),m) =>
    h.insert u (h.getD u 0 + m * targets.getD v 0)) {}
  posts.foldl (fun a (u,_,score) => plus a (weighted (weight.getD u 0) score)) zero = refBridgeAgg posts es t

/-- C3: restrict each alias of posts to precisely the endpoints requested by selected
link types. A source-only id needs no target entry, and conversely for a target-only id. -/
def EndpointDemand.Goal : Prop := ∀ (posts : List PostRec) (es : List LinkRec) t,
  let edges := es.filter (fun e => e.2.2 == t)
  let sources := (edges.map Prod.fst).eraseDups
  let targets := (edges.map (fun e => e.2.1)).eraseDups
  let src := posts.filter (fun p => sources.contains p.1)
  let dst := posts.filter (fun p => p.2.1 && targets.contains p.1)
  summarize (edges.flatMap (fun (u,v,_) =>
    (src.filter (·.1 == u)).flatMap (fun p =>
      List.replicate (dst.countP (·.1 == v)) p.2.2))) = refBridgeAgg posts es t

/-- C2: aggregate repeated comments before routing by post. Owner-role and
commenter-role multiplicities come from different filtered projections of users;
neither post ids nor user ids have to be globally unique. -/
def CommentQuotient.Goal : Prop := ∀ (users : List UserRow) (posts comments : List (Nat × Nat)) owner,
  let commenters := histogram ((users.filter UserRow.repOk).map UserRow.id)
  let owners := users.countP (fun u => u.yrOk && u.id == owner)
  let mass := (histogram comments).toList.foldl (fun (m : Std.HashMap Nat Nat) ((p,u),w) =>
    m.insert p (m.getD p 0 + w * commenters.getD u 0)) {}
  owners * ((posts.filter (·.2 == owner)).map (fun p => mass.getD p.1 0)).sum =
    refDualUserCount users posts comments owner

/-- C4: classify a hash bucket by whether all entries have one spelling; compare
a singleton spelling once or fall back to the full collision list. Never match SQL
NULL in the LEFT JOIN. Outer padding still applies to misses and hash collisions. -/
inductive Bucket where
  | empty
  | uniform (label : String) (count : Nat)
  | mixed (labels : List String)
def classify : List String → Bucket
  | [] => .empty
  | h::tail => if tail.all (· == h) then .uniform h (tail.length+1) else .mixed (h::tail)
def bucketWeight (b : Bucket) (key : String) : Nat := max 1 (match b with
  | .empty => 0
  | .uniform s n => if key == s then n else 0
  | .mixed labels => labels.count key)
def CollisionBucket.Goal : Prop := ∀ (tags : List (Option String)) (hashKey : String → Nat)
    (selected : List (Option String × Option Int)),
  let buckets := (tags.filterMap id).foldl (fun (m : Std.HashMap Nat (List String)) s =>
    m.insert (hashKey s) (s::m.getD (hashKey s) [])) {}
  let compiled := buckets.toList.foldl (fun (m : Std.HashMap Nat Bucket) (h,labels) =>
    m.insert h (classify labels)) {}
  selected.foldl (fun a (k,v) => plus a (weighted
    (k.elim 1 (fun s => bucketWeight (compiled.getD (hashKey s) .empty) s)) v)) zero =
    summarize (selected.flatMap (fun (k,v) =>
      List.replicate (max 1 (tags.countP (fun t => k.isSome && t == k))) v))

/-- L1: intern identical country multisets of incoming edges. Aggregate active
friend multiplicity per interned profile, then scatter to countries once per profile.
Canonicalizing by set would lose parallel knows/source/place paths. -/
def CountryProfiles.Goal : Prop := ∀ (edges : List (Nat × Nat)) (active : List Nat) country,
  let adjacency := edges.foldl (fun (m : Std.HashMap Nat (List Nat)) (f,c) =>
    m.insert f (c::m.getD f [])) {}
  let profiles := (histogram active).toList.foldl (fun (m : Std.HashMap (List Nat) Nat) (f,w) =>
    let cs := (adjacency.getD f []).mergeSort
    m.insert cs (m.getD cs 0 + w)) {}
  profiles.toList.foldl (fun n (cs,w) => n + w * cs.count country) 0 =
    (edges.map (fun (f,c) => if c == country then active.count f else 0)).sum

#guard bucketWeight (classify ["sql","sql"]) "nosql" == 1 && bucketWeight (classify ["sql","sql","nosql"]) "sql" == 2
#guard H2o.finish (H2o.contribute {} 0 ⟨1,0,0⟩ ⟨2,2,0⟩) 0 == (2,none,some 0)

end TinylambOpt.ResearchGoals.DuckDBGaps.BridgeStates
