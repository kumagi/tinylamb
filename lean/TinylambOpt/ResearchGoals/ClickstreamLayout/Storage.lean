import TinylambOpt.ResearchGoals.Compression.Wire

/-!
These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.
-/

namespace TinylambOpt.ResearchGoals.ClickstreamLayout.Storage
open TinylambOpt.ResearchGoals.Compression.Wire (ubytes)

namespace RunSidecar
/-! Physical row order is a permutation of original ordinals. Each maximal
ascending run can be recovered by scanning a single logical-order label stream.
An identity tag alone covers an already clustered granule. -/
def runs : List Nat → List (List Nat)
  | [] => []
  | x::xs => match runs xs with
    | (y::ys)::rest => if x < y then (x::y::ys)::rest else [x]::(y::ys)::rest
    | rs => [x]::rs

def labels (rs : List (List Nat)) (n : Nat) :=
  (List.range n).map (fun i => rs.findIdx (fun r => r.contains i))

def recover (tags : List Nat) (k : Nat) :=
  (List.range k).flatMap fun r =>
    tags.zipIdx.filterMap (fun (t,i) => if t == r then some i else none)

/-- Can clustered compaction replace full-width physical ordinals with one
label per logical row? Require exact inversion and the minimum number of
labels among representations with contiguous, strictly ascending lanes. -/
def Goal : Prop := ∀ (ids : List Nat), ids.Perm (List.range ids.length) →
  let rs := runs ids
  recover (labels rs ids.length) rs.length = ids ∧
    ∀ other : List (List Nat), other.flatten = ids →
      (∀ lane ∈ other, lane.Pairwise (· < ·)) → rs.length ≤ other.length

#guard labels (runs [0,3,1,2]) 4 == [0,1,1,0]
#guard recover [0,1,1,0] 2 == [0,3,1,2]
end RunSidecar

namespace TieOrientation
/-! Within an equal clustering key, two candidate physical orders are offered.
The front-coded text stream carries its previous entry across group boundaries.
The enclosing granule supplies a row-ID mapping, priced outside this objective. -/
abbrev Word := List UInt8
def lcp : Word → Word → Nat
  | a::as, b::bs => if a == b then 1 + lcp as bs else 0
  | _, _ => 0

def price (a b : Word) :=
  let shared := lcp a b
  let suffix := b.length - shared
  ubytes shared + ubytes suffix + suffix

def charge (previous : Word) : List Word → Nat
  | [] => 0
  | b::bs => price previous b + charge b bs

def orders : List (List Word) → List (List Word)
  | [] => [[]]
  | g::gs => (orders gs).flatMap (fun tail => [g ++ tail, g.reverse ++ tail])

-- A DP state records the two possible last words and the cheapest cost to each.
def optimum (groups : List (List Word)) :=
  let states := groups.foldl (fun (states : List (Word × Nat)) group =>
    if group.isEmpty then states else [group,group.reverse].map fun order =>
      (order.getLast?.getD [], (states.map (fun (w,c) => c + charge w order)).min?.getD 0))
    [([],0)]
  (states.map Prod.snd).min?.getD 0

/-- Does a two-state writer find the smallest front-coded payload over every
sequence of tie orientations? Includes the first restart and varint cliffs. -/
def Goal : Prop := ∀ groups,
  some (optimum groups) = ((orders groups).map (charge [])).min?

#guard optimum [[[1,2,3]],[],[[9],[1,2,4]]] < charge [] [[1,2,3],[9],[1,2,4]]
end TieOrientation

namespace DictionaryAdmission
/-! Immutable frames may opt into one trained dictionary. The root can charge
its stored bytes. Each internal node charges a fixed cold-read cost if any
descendant uses the dictionary. This is a laminar workload model. All costs
have been converted to the same units. -/
inductive Tree where
  | frame (id : Nat) (saved : Int)
  | bundle (activation : Nat) (left right : Tree)

def ids : Tree → List Nat
  | .frame i _ => [i]
  | .bundle _ l r => ids l ++ ids r

def score (useDict : Nat → Bool) : Tree → Bool × Int
  | .frame i s => if useDict i then (true,s) else (false,0)
  | .bundle fee l r =>
    let (a,x) := score useDict l
    let (b,y) := score useDict r
    (a || b, x+y-(if a || b then (fee : Int) else 0))

def best : Tree → Int
  | .frame _ s => max 0 s
  | .bundle c l r => max 0 (best l + best r - (c : Int))

/-- Select frames that amortize both dictionary storage and cold projection
cost. The tree recurrence must bound every admission and attain that bound. -/
def Goal : Prop := ∀ t, (ids t).Nodup →
  (∀ useDict, (score useDict t).2 ≤ best t) ∧ ∃ useDict, (score useDict t).2 = best t

#guard best (.bundle 6 (.frame 0 4) (.frame 1 4)) == 2
end DictionaryAdmission

namespace HashExceptions
/-! Store an observed hash per URL dictionary entry and only nonmatching row
ordinals as exceptions. Hash collisions across dictionary entries are allowed.
This model prices a fixed-width exception record; NULLs are handled upstream. -/
abbrev Row := Nat × UInt64
def mode (xs : List UInt64) :=
  xs.foldl (fun winner h => if xs.count h > xs.count winner then h else winner) 0

def predicted (rows : List Row) (code : Nat) :=
  mode (rows.filterMap (fun (k,h) => if k == code then some h else none))

def exceptions (rows : List Row) (predict : Nat → UInt64) :=
  rows.zipIdx.filterMap fun ((k,h),i) => if predict k == h then none else some (i,h)

def selectHashes (codes : List Nat) (predict : Nat → UInt64)
    (patch : List (Nat × UInt64)) (wanted : UInt64 → Bool) :=
  codes.zipIdx.filterMap fun (k,i) =>
    let h := (patch.lookup i).getD (predict k)
    if wanted h then some i else none

/-- Is the per-code mode optimal for the number of stored exceptions, while
a hash predicate over codes and exceptions returns the original occurrence IDs? -/
def Goal : Prop := ∀ rows wanted,
  let predict := predicted rows
  let patch := exceptions rows predict
  (∀ alt, patch.length ≤ (exceptions rows alt).length) ∧
    selectHashes (rows.map Prod.fst) predict patch wanted =
      rows.zipIdx.filterMap (fun ((_,h),i) => if wanted h then some i else none)

#guard exceptions [(1,7),(1,8),(2,7),(1,7)] (predicted [(1,7),(1,8),(2,7),(1,7)]) == [(1,8)]
end HashExceptions

namespace ClockWindow
open TinylambOpt.ResearchGoals.Compression.Wire (byteCount sbytes)
/-! Signed inter-clock differences may have a tight central range and very
large exceptions. The base has a zigzag-varint price. Each row has a w-bit
slot, and outliers also store a fixed-width ordinal and the original delta. -/
def cost (ds : List Int) (w exceptionBytes : Nat) (low : Int) :=
  let span : Int := (2^w : Nat) - (1 : Int)
  let misses := ds.countP (fun d => decide (d < low ∨ low+span < d))
  1 + sbytes low + byteCount (ds.length*w) + misses*exceptionBytes

def anchors (ds : List Int) (w : Nat) :=
  let span : Int := (2^w : Nat) - (1 : Int)
  0 :: ds.flatMap (fun d => [d, d-span])

/-- For fixed slot width, can a writer find a globally optimal residual
window by testing only zero and the two coverage endpoints of each value?
The serialized base itself counts, including its signed varint cliffs. -/
def Goal : Prop := ∀ ds w exceptionBytes,
  ∃ low ∈ anchors ds w, ∀ other, cost ds w exceptionBytes low ≤ cost ds w exceptionBytes other

#guard cost [63,64,65] 2 10 62 < cost [63,64,65] 2 10 64
#guard (anchors [-80,-79,90] 1).contains (-81)
end ClockWindow

namespace ClockForest
/-! Store each time column either independently or as a delta against an
already decodable column. Costs are actual frame sizes, including headers.
Direction matters even when subtraction is lossless in both directions. -/
abbrev Parent := Nat → Option Nat
def legal (cols : List Nat) (parent : Parent) :=
  ∃ rank : Nat → Nat, ∀ c ∈ cols, ∀ p, parent c = some p → p ∈ cols ∧ rank p < rank c

def total (price : Nat → Option Nat → Nat) (cs : List Nat) (parent : Parent) :=
  (cs.map (fun c => price c (parent c))).sum

-- A subset DP can memoize by the set already written (the remaining set is its complement).
def cheapest (price : Nat → Option Nat → Nat) (done : List Nat) : Nat → List Nat → Nat
  | 0, _ => 0
  | fuel+1, todo => (todo.map (fun c =>
      let choices := price c none :: done.map (fun p => price c (some p))
      choices.min?.getD 0 + cheapest price (c::done) fuel (todo.erase c))).min?.getD 0

/-- Can the writer minimize actual directed payload bytes over *acyclic*
anchor choices? The optimum must be decodable and beat every legal assignment.
Scan-time read amplification is a separate objective. -/
def Goal : Prop := ∀ (cols : List Nat) price, cols.Nodup →
  let optimum := cheapest price [] cols.length cols
  (∃ p, legal cols p ∧ total price cols p = optimum) ∧
    ∀ p, legal cols p → optimum ≤ total price cols p

#guard cheapest (fun _ p => if p.isNone then 8 else 1) [] 3 [0,1,2] == 10
end ClockForest

end TinylambOpt.ResearchGoals.ClickstreamLayout.Storage
