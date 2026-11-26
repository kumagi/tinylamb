import Std

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.
-/

namespace TinylambOpt.ResearchGoals.EngineDecisions.Executor

namespace RunOffset
/-! Each input group is one complete, consecutive prefix-peer run. Prefixes
are already in query order. Rows have a suffix key and occurrence ID. Keys
are total, and skipping a run does not suppress an observable evaluation error. -/
abbrev Row := Nat × Nat
abbrev Output := Nat × Row
def ordered (xs : List Row) := xs.mergeSort (fun a b => a.1 ≤ b.1)
def takeTies {α β : Type} [BEq β] (key : α → β) (n : Nat) (xs : List α) :=
  let kept := xs.take n
  match kept.getLast? with
  | none => []
  | some last => kept ++ (xs.drop n).takeWhile (fun x => key x == key last)
def slice (off n : Nat) : List (List Row × Nat) → List Output
  | [] => []
  | (rs,p)::gs =>
    if n == 0 then [] else
    if rs.length ≤ off then slice (off-rs.length) n gs else
      let localRows := (ordered rs).drop off
      if n ≤ rs.length-off then (takeTies Prod.fst n localRows).map (p,·)
      else localRows.map (p,·) ++ slice 0 (n-(rs.length-off)) gs

/-- Skip sorting entire runs covered by OFFSET and stop after the boundary
run, preserving suffix ties only within their own prefix. -/
def Goal : Prop := ∀ (runs : List (List Row)) off n,
  let groups := runs.zipIdx
  let full := groups.flatMap fun (rs,p) => (ordered rs).map (p,·)
  slice off n groups = takeTies (fun (p,r) => (p,r.1)) n (full.drop off)

#guard slice 1 1 [[(7,0)],[(1,1),(1,2)],[(1,3)]].zipIdx ==
  [(1,(1,1)),(1,(1,2))]
end RunOffset

namespace HotRunTiles
/-! One equal, non-NULL merge key with left and right occurrence counts.
A pure residual may reject arbitrary pairs. Tiles can be executed by different
workers. The full outer result observes a bag of occurrence IDs. -/
abbrev PairId := Nat × Nat
abbrev Out := Option Nat × Option Nat
def tile (l r width t : Nat) :=
  (List.range (min width (l*r-t*width))).map fun j =>
    let ordinal := t*width+j
    (ordinal/r, ordinal%r)
def finish (l r : Nat) (hits : List PairId) : List Out :=
  hits.map (fun (i,j) => (some i,some j)) ++
  ((List.range l).filter (fun i => !hits.any (fun p => p.1 == i))).map (fun i => (some i,none)) ++
  ((List.range r).filter (fun j => !hits.any (fun p => p.2 == j))).map (fun j => (none,some j))
def outerRef (l r : Nat) (pass : PairId → Bool) : List Out :=
  (List.range l).flatMap (fun i =>
    let js := (List.range r).filter (fun j => pass (i,j))
    if js.isEmpty then [(some i,none)] else js.map (fun j => (some i,some j))) ++
  ((List.range r).filter (fun j => !(List.range l).any (fun i => pass (i,j)))).map (fun j => (none,some j))

/-- Split a single hot key across workers in pair-ordinal space and union
matched occurrence bits *after* residual evaluation before outer emission. -/
def Goal : Prop := ∀ l r width (order : List Nat) pass, 0 < width →
  order.Perm (List.range ((l*r+width-1)/width)) →
  let hits := order.flatMap fun t => (tile l r width t).filter pass
  (finish l r hits).Perm (outerRef l r pass)

#guard finish 1 2 [(0,1)] == [(some 0,some 1),(none,some 0)]
end HotRunTiles

namespace DistinctExcludeTies
/-! A clipped frame of row occurrences in one partition. EXCLUDE TIES
drops peers except the current occurrence, provided that it lies in the frame. -/
structure R where
  id : Nat
  peer : Nat
  value : Option Int

def distinctVals (xs : List R) := (xs.filterMap R.value).eraseDups
def estimate (frame : List R) (current : R) :=
  let peers := frame.filter (fun r => r.peer == current.peer)
  let allValues := frame.filterMap R.value
  let peerValues := peers.filterMap R.value
  let exclusive := peerValues.eraseDups.filter (fun v => peerValues.count v == allValues.count v)
  let keepCurrent := (peers.any (fun r => r.id == current.id)) && current.value.any exclusive.contains
  allValues.eraseDups.length - exclusive.length + keepCurrent.toNat

/-- Maintain frame and peer frequency counts to compute COUNT(DISTINCT)
EXCLUDE TIES, restoring the current value only if it lost its last occurrence. -/
def Goal : Prop := ∀ frame current,
  (∀ r ∈ frame, r.id = current.id → r = current) →
  estimate frame current = (distinctVals (frame.filter (fun r =>
    r.peer != current.peer || r.id == current.id))).length

#guard estimate [⟨1,0,some 7⟩,⟨2,0,some 7⟩,⟨3,1,some 7⟩] ⟨1,0,some 7⟩ == 1
#guard estimate [⟨1,0,some 7⟩] ⟨2,0,some 7⟩ == 0
end DistinctExcludeTies

namespace RangeFingers
/-! Two independent cursors into the sorted right keys. A successful
comparison consumes a key, a failed one ends a seek. No binary search is
charged in this specific merge-style alternative to ZigZagJoin's bounds. -/
def seek (p : Int → Bool) : List Int → Nat × List Int × Nat
  | [] => (0,[],0)
  | x::xs =>
    if p x then
      let (n,rest,work) := seek p xs
      (n+1,rest,work+1)
    else (0,x::xs,1)

def sweep (lo hi : Nat) (left lowTail highTail : List Int) : List (Nat × Nat) × Nat :=
  match left with
  | [] => ([],0)
  | x::xs =>
    let (a,l,c) := seek (· < x) lowTail
    let (b,h,d) := seek (· ≤ x) highTail
    let (rest,work) := sweep (lo+a) (hi+b) xs l h
    ((lo+a,hi+b)::rest,c+d+work)

/-- An ordered probe morsel can replace repeated lower/upper bounds by two
fingers, producing exact equal ranges within a linear comparison budget. -/
def Goal : Prop := ∀ (left right : List Int),
  left.Pairwise (· ≤ ·) → right.Pairwise (· ≤ ·) →
  let (bounds,work) := sweep 0 0 left right right
  bounds = left.map (fun x => (right.countP (· < x),right.countP (· ≤ x))) ∧
    work ≤ 2*(left.length + right.length)

#guard (sweep 0 0 [2,2,4] [1,2,2,4] [1,2,2,4]).1 == [(1,3),(1,3),(3,4)]
end RangeFingers

namespace FlushChoice
/-! One admission stall. A staged partition can be flushed in full. Reclaimed
in-memory bytes and physical write cost include different padding and headers.
The I/O unit is abstract; all costs are nonnegative. -/
abbrev Bucket := Nat × Nat
def bestOption : Option Nat → Option Nat → Option Nat
  | some a,some b => some (min a b)
  | some a,none | none,some a => some a
  | none,none => none
def flushCost : List Bucket → Nat → Option Nat
  | [], need => if need == 0 then some 0 else none
  | (bytes,io)::bs, need =>
    if need == 0 then some 0 else
    bestOption (flushCost bs need) ((flushCost bs (need-bytes)).map (io+·))

def subsets : List Bucket → List (List Bucket)
  | [] => [[]]
  | b::bs =>
    let rest := subsets bs
    rest ++ rest.map (b::·)

/-- An admission controller can price the cheapest subset with a DP keyed
by partition ordinal and remaining grant deficit. Covers infeasibility,
and asks for a realizable cost as well as a lower bound. -/
def Goal : Prop := ∀ bs need,
  let feasible := (subsets bs).filter (fun xs => need ≤ (xs.map Prod.fst).sum)
  flushCost bs need = (feasible.map (fun xs => (xs.map Prod.snd).sum)).min?

#guard flushCost [(9,4),(4,1),(3,1)] 7 == some 2
end FlushChoice

namespace SealedBuild
/-! Concurrent pure equality semijoin. A sealed build partition accepts
no further rows. Other partitions may receive any future key, and a NULL
probe cannot match. Only ordinary two-valued EXISTS is observed. -/
def decideNow (part : Nat → Nat) (sealed built : List Nat) : Option Nat → Option Bool
  | none => some false
  | some k => if built.contains k then some true else
                if sealed.contains (part k) then some false else none
def existsMatch (q : Option Nat) (ks : List Nat) := q.any ks.contains

/-- Return a final decision before the global build barrier exactly when all
legal future build tails agree. This licenses both positive early emission
and partition-local negative/anti emission. -/
def Goal : Prop := ∀ part sealed built q b, decideNow part sealed built q = some b ↔
  ∀ tail : List Nat, (∀ k ∈ tail, part k ∉ sealed) → existsMatch q (built++tail) = b

#guard decideNow (· % 2) [0] [1] (some 3) == none
#guard decideNow (· % 2) [0] [1] (some 4) == some false
end SealedBuild

namespace DemandCache
/-! Pure correlated filter with deterministic errors in a fixed snapshot.
Rows arrive in outer order. A LIMIT stops at the kth accepted occurrence.
The work counter measures actual inner evaluations. -/
abbrev Verdict := Except Nat Bool
inductive Stop where
  | eof | enough | fault (ordinal code : Nat)
  deriving BEq, DecidableEq
structure Result where
  emitted : List Nat
  stop : Stop
  deriving BEq, DecidableEq
def takePassing (test : Nat → Verdict) : Nat → Nat → List Nat → Result
  | 0, _, _ => ⟨[],.enough⟩
  | _+1, _, [] => ⟨[],.eof⟩
  | n+1, i, k::ks =>
    match test k with
    | .error e => ⟨[],.fault i e⟩
    | .ok false => takePassing test (n+1) (i+1) ks
    | .ok true => let rest := takePassing test n (i+1) ks
                  {rest with emitted := k::rest.emitted}

def cached (test : Nat → Verdict) (memo : List (Nat × Verdict)) :
    Nat → Nat → List Nat → Result × Nat
  | 0, _, _ => (⟨[],.enough⟩,0)
  | _+1, _, [] => (⟨[],.eof⟩,0)
  | n+1, i, k::ks =>
    let (v,next,work) := match memo.lookup k with
      | some v => (v,memo,0)
      | none => let v := test k; (v,(k,v)::memo,1)
    match v with
    | .error e => (⟨[],.fault i e⟩,work)
    | .ok pass =>
      let (rest,extra) := cached test next (if pass then n else n+1) (i+1) ks
      let ids := if pass then k::rest.emitted else rest.emitted
      (⟨ids,rest.stop⟩,work+extra)
termination_by _ _ ks => ks.length

/-- Cache a correlated result once per demanded key, preserving the output
prefix and first demanded fault. Price only distinct keys reached before stop. -/
def Goal : Prop := ∀ (test : Nat → Verdict) (ks : List Nat) n,
  let ref := takePassing test n 0 ks
  let (out,work) := cached test [] n 0 ks
  let visited := match ref.stop with
    | .eof => ks
    | .fault i _ => ks.take (i+1)
    | .enough => if n == 0 then [] else ks.take ((ks.zipIdx.filter
        (fun (item : Nat × Nat) => test item.1 matches .ok true))[n-1]?.map (fun (item : Nat × Nat) => item.2+1) |>.getD 0)
  out = ref ∧ work = visited.eraseDups.length

private def verdict (k : Nat) : Verdict := if k == 9 then .error 17 else .ok (k == 2)
#guard (cached verdict [] 1 0 [1,1,2,9]).2 == 2
#guard (takePassing verdict 1 0 [1,1,9,2]).stop == .fault 2 17
end DemandCache

end TinylambOpt.ResearchGoals.EngineDecisions.Executor
