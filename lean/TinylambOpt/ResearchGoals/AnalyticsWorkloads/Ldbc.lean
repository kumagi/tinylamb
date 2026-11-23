import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common
import TinylambOpt.ResearchGoals.RecursiveDelta

namespace TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ldbc
open TinylambOpt.ResearchGoals.RecursiveDelta (index)

/-- L1. Upstream joins supply one (friend,country-code) edge per knows/source/place
match. Qualifying p2/like/message matches form a bag of active friends. Traverse
only their incoming lists in an existing reverse index, once per distinct friend. -/
abbrev Edge := Nat × Nat
def pull (incoming : Std.HashMap Nat (List Nat)) (active : List Nat) :=
  (histogram active).toList.foldl (fun (groups,visited) (person,w) =>
    let block := incoming.getD person []
    let next := block.foldl (fun h c => h.insert c (h.getD c 0 + w)) groups
    (next,visited+block.length)) (({} : Std.HashMap Nat Nat),0)

def ActiveFriends.Goal : Prop := ∀ es active country,
  let (groups,reads) := pull (index es) active
  groups.getD country 0 = (es.flatMap (fun (person,c) =>
    (active.filter (· == person)).map fun _ => c)).count country ∧
    reads = (es.countP fun e => active.contains e.1) ∧ reads ≤ es.length

/-- L2. Sorted/clustered messages can carry the qualifying member count across
adjacent equal forum ids. The returned probe count tracks physical lookups. -/
abbrev Message := Nat × Int × Option Int -- forum, year, score
def carried (members : Nat → Nat) (rows : List Message) : Std.HashMap (Nat × Int) Totals × Nat :=
  let (_,_,result,probes) := rows.foldl (fun (prev,w,st,n) (f,y,v) =>
    let same := prev == some f
    let weight := if same then w else members f
    let updated := if weight == 0 then st else
      st.insert (f,y) (plus (st.getD (f,y) zero) (weighted weight v))
    (some f,weight,updated,n+(!same).toNat)) (none,0,{},0)
  (result,probes)

def ForumRuns.Goal : Prop := ∀ members rows key,
  let (groups,reads) := carried members rows
  groups.getD key zero = summarize (rows.flatMap fun (f,y,v) =>
    if (f,y) == key then List.replicate (members f) v else []) ∧
    reads = (rows.zipIdx.countP fun (r,i) => i == 0 || (rows[i-1]?.map Prod.fst) != some r.1)

/-- L3. A single physical scan can meet forward parent references. Hold unresolved
positive reply scores by parent id, and flush only their aggregate when the parent
appears. Insert the current parent before routing its own reply to handle self-links. -/
structure Reply where
  id : Nat
  year : Int
  parent : Option Nat
  score : Option Int

structure State where
  years : Std.HashMap Nat Int := {}
  pending : Std.HashMap Nat Totals := {}
  groups : Std.HashMap Int Totals := {}

def credit (s : State) (year : Int) (v : Totals) :=
  if 2010 ≤ year && year ≤ 2014 then
    {s with groups := s.groups.insert year (plus (s.groups.getD year zero) v)}
  else s

def arrive (s : State) (r : Reply) : State := Id.run do
  let mut next := {s with years := s.years.insert r.id r.year, pending := s.pending.erase r.id}
  if let some waiting := s.pending[r.id]? then next := credit next r.year waiting
  if let some p := r.parent then
    if r.score.any (fun v => 0 < v) then
      let part := weighted 1 r.score
      if let some y := next.years[p]? then next := credit next y part
      else next := {next with pending := next.pending.insert p (plus (next.pending.getD p zero) part)}
  return next

def replyRef (rs : List Reply) (year : Int) := summarize (rs.flatMap fun a =>
  if a.year == year && 2010 ≤ year && year ≤ 2014 then
    (rs.filter (fun b => b.parent == some a.id && b.score.any (fun x => 0 < x))).map Reply.score
  else [])

def ForwardReplies.Goal : Prop := ∀ rs year, (rs.map Reply.id).Nodup →
  (rs.foldl arrive {}).groups.getD year zero = replyRef rs year

/-- L4. Probe forum membership after filtering and grouping messages. Store qualifying
message count separately from non-NULL scores. Duplicate forum ids scale both. -/
structure ForumMessage where
  forum : Option Nat
  year : Int
  parent : Option Nat
  score : Option Int
def afterGrouping (fs : List Nat) (ms : List ForumMessage) (k : Nat) :=
  let passed := ms.filter fun m => m.forum == some k && m.year == 2012 && m.parent.isSome
  let a := summarize (passed.map ForumMessage.score)
  let n := fs.count k
  (⟨a.cnt*n,a.nn*n,a.sum*n⟩ : Totals)

def LateForumWeight.Goal : Prop := ∀ fs ms k,
  afterGrouping fs ms k = summarize (fs.flatMap fun f => if f != k then [] else
    (ms.filter (fun m => m.forum == some f && m.year == 2012 && m.parent.isSome)).map ForumMessage.score)

/-- L5. A CSR directory yields each person's out-degree using adjacent offsets.
No destination payload is needed. People already carry their eligible place matches. -/
def offsets (edges : List Edge) (width : Nat) :=
  let degree := histogram (edges.map Prod.fst)
  ((List.range width).scanl (fun n i => n+degree.getD i 0) 0).toArray

abbrev Resident := Nat × String × Option Int
def degreeAvg (dir : Array Nat) (ps : List Resident) (label : String) :=
  ps.foldl (fun st (i,c,age) => if c != label then st else
    plus st (weighted (dir[i+1]?.getD 0 - dir[i]?.getD 0) age)) zero

def DirectoryDegree.Goal : Prop := ∀ es width ps label, (∀ e ∈ es, e.1 < width) →
  degreeAvg (offsets es width) ps label = summarize (ps.flatMap fun (id,c,age) =>
    if c == label then (es.filter (fun e => e.1 == id)).map (fun _ => age) else [])

#guard (let (g,n) := pull (index [(1,7),(1,7),(1,3),(3,7)]) [1,1,8]; (g.getD 7 0,n) == (4,3))
#guard (
  let (gs,n) := carried (fun f => if f == 2 then 3 else 0)
    [(2,2011,none),(2,2012,some 4),(1,2011,some 8)]
  gs.getD (2,2011) zero == ⟨3,0,0⟩ && !gs.contains (1,2011) && n == 2)
private def messages : List Reply := [⟨1,2010,some 2,some 3⟩,⟨2,2012,some 2,some 4⟩,
  ⟨3,2015,some 2,some 0⟩,⟨4,2011,some 9,some 8⟩]
#guard (messages.foldl arrive {}).groups.getD 2012 zero == ⟨2,2,7⟩ && replyRef messages 2012 == ⟨2,2,7⟩
#guard afterGrouping [2,2] [⟨some 2,2012,some 99,none⟩,⟨some 2,2012,none,some 40⟩] 2 == ⟨2,0,0⟩
#guard degreeAvg (offsets [(0,7),(0,7),(2,7)] 3) [(0,"US",some 30),(2,"US",none),(3,"US",some 99)] "US" == ⟨3,2,60⟩

end TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ldbc
