import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common
import TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum

/-!
These models describe plan alternatives for analytical SQL, with row multiplicity and
aggregate state made explicit.
-/

namespace TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins
open SemijoinGuardedCheckedEagerSum (i64Min i64Max inI64)

/-- C1/H5. A comment contribution repeats once per qualifying vote. Given a valid
starting accumulator, locate the first failed addition by division, even for negative
scores, without performing the repeated checked additions. The failure index is 1-based. -/
def repeatChecked (acc v : Int) (n : Nat) : Except Nat Int :=
  let safe := if 0 < v then ((i64Max-acc)/v).toNat
              else if v < 0 then ((acc-i64Min)/(-v)).toNat else n
  if safe < n then .error (safe+1) else .ok (acc+v*n)

def repeatedReference (acc v : Int) (n : Nat) : Except Nat Int := Id.run do
  let mut s := acc
  for i in List.range n do
    s := s+v
    if !inI64 s then return .error (i+1)
  return .ok s

def RepeatedScore.Goal : Prop := ∀ acc v n, inI64 acc → inI64 v →
  repeatChecked acc v n = repeatedReference acc v n

/-- C2. After verifying unique dimension/post ids, fuse the two users roles into an
owner-by-post map and an eligible-commenter set. Missing owners never form a group. -/
structure User where
  id : Nat
  year : Int
  rep : Int
abbrev Post := Nat × Nat -- post id, owner id
abbrev Comment := Nat × Nat -- post id, commenter id

def routed (users : List User) (ps : List Post) (cs : List Comment) :=
  let owners := Std.HashSet.ofList ((users.filter (fun u => 2012 ≤ u.year)).map User.id)
  let eligible := Std.HashSet.ofList ((users.filter (fun u => 1000 < u.rep)).map User.id)
  let route := Std.HashMap.ofList (ps.filter (fun p => owners.contains p.2))
  cs.filterMap fun (post,who) => if eligible.contains who then route[post]? else none

def RoleRouting.Goal : Prop := ∀ users ps cs, (users.map User.id).Nodup → (ps.map Prod.fst).Nodup →
  (routed users ps cs).Perm (cs.flatMap fun (post,who) =>
    (ps.filter (fun p => p.1 == post)).flatMap fun p =>
    (users.filter (fun u => u.id == p.2 && 2012 ≤ u.year)).flatMap fun owner =>
    (users.filter (fun u => u.id == who && 1000 < u.rep)).map fun _ => owner.id)

/-- C3. Each post id stores a source aggregate and a separate target frequency.
An edge performs two scalar lookups, even for self-links and duplicate post ids. -/
abbrev PostValue := Nat × Bool × Option Int -- id, type=1, score
abbrev Link := Nat × Nat × Nat -- source, target, link_type

def linkState (ps : List PostValue) (edges : List Link) (kind : Nat) : Totals :=
  let src := ps.foldl (fun (h : Std.HashMap Nat Totals) (k,_,v) =>
    h.insert k (plus (h.getD k zero) (weighted 1 v))) {}
  let dst := histogram ((ps.filter (fun p => p.2.1)).map Prod.fst)
  edges.foldl (fun st (u,v,t) =>
    if t != kind then st else
      let a := src.getD u zero
      let n := dst.getD v 0
      plus st ⟨a.cnt*n,a.nn*n,a.sum*n⟩) zero

def EndpointProfiles.Goal : Prop := ∀ ps es k,
  linkState ps es k = summarize (es.flatMap fun (u,v,t) =>
    if t != k then [] else (ps.filter (fun p => p.1 == u)).flatMap fun a =>
      (ps.filter (fun p => p.1 == v && p.2.1)).map fun _ => a.2.2)

/-- C4. Every selected post contributes once. A sparse correction table contains
only tag names with multiplicity above one. SQL NULL never matches another NULL. -/
abbrev Hit := Option String × Option Int
def extras (tags : List (Option String)) :=
  ((histogram (tags.filterMap id)).toList.filterMap fun (s,n) =>
    if n > 1 then some (s,n-1) else none)

def corrected (hits : List Hit) (tags : List (Option String)) :=
  let weight : Option String → Nat := match extras tags with
    | [] => fun _ => 1
    | [(name,n)] => fun k => if k == some name then n+1 else 1
    | more =>
      let extra := Std.HashMap.ofList more
      fun k => 1 + k.elim 0 (extra.getD · 0)
  hits.foldl (fun st (tag,v) => plus st (weighted (weight tag) v)) zero

def SparseOuterCorrection.Goal : Prop := ∀ hits tags,
  corrected hits tags = summarize (hits.flatMap fun (t,v) =>
    List.replicate (max 1 (tags.countP fun s => s.isSome && s == t)) v)

/-- C5. Inspect only badges of qualifying post owners. Caching one eligible
user multiplicity preserves duplicate users in a schema without a declared PK. -/
def badgeDemand (postOwners eligible badgeOwners : List Nat) :=
  let demand := Std.HashSet.ofList postOwners
  let users := histogram (eligible.filter demand.contains)
  histogram (badgeOwners.filter users.contains) |>.toList.map (fun (k,b) => (k, b * users.getD k 0))

def DemandBadges.Goal : Prop := ∀ (ps us bs : List Nat) k, k ∈ ps →
  (Std.HashMap.ofList (badgeDemand ps us bs)).getD k 0 =
    (us.filter (· == k)).length * (bs.filter (· == k)).length

/-- C5. ORDER BY year after grouping by (year,type) needs only the year order.
Bucket placement preserves the complete grouped payload, also on tied years. -/
abbrev Group := Int × Nat × Nat
def yearBuckets (rows : List Group) :=
  let buckets := rows.foldl (fun (h : Std.HashMap Int (List Group)) r =>
    h.insert r.1 (r::h.getD r.1 [])) {}
  buckets.keys.mergeSort.flatMap fun y => (buckets.getD y []).reverse

def YearOnlyOrder.Goal : Prop := ∀ rows,
  yearBuckets rows = rows.mergeSort (fun a b => a.1 ≤ b.1)

#guard (match repeatChecked (i64Max-2) 2 3 with | .error 2 => true | _ => false)
#guard (match repeatChecked (i64Min+2) (-1) 3 with | .error 3 => true | _ => false)
#guard routed [⟨1,2012,5⟩,⟨2,2010,1001⟩] [(7,1),(8,3)] [(7,2),(7,2),(7,1),(8,2)] == [1,1]
#guard linkState [(2,true,none),(2,true,some 3)] [(2,2,3)] 3 == ⟨4,2,6⟩
#guard corrected [(none,some 7),(some "sql",some 3),(some "miss",none)] [none,none,some "sql",some "sql"] == ⟨4,3,13⟩
#guard badgeDemand [2,2] [2,2,3] [2,2,3,3] == [(2,4)]
#guard yearBuckets [(2012,3,1),(2010,2,8),(2012,1,4)] == [(2010,2,8),(2012,3,1),(2012,1,4)]

end TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins
