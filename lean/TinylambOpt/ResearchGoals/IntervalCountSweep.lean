import Std

/-!
These claims compute interval counts from ordered endpoints. A SQL range operator can
use a sweep instead of visiting every interval-row pair.

Modeled claims

- `Goal` — `∀ spans points, sweep spans points = reference spans points`
-/

namespace TinylambOpt.ResearchGoals.IntervalCountSweep

abbrev Cell := Option Int
abbrev Span := Cell × Cell

def contains (span : Span) (p : Cell) : Bool :=
  match span, p with
  | (some lo, some hi), some t => lo ≤ t && t < hi
  | _, _ => false

def reference (spans : List Span) (points : List Cell) :=
  points.map fun p => (spans.filter (contains · p)).length

inductive Op where
  | start | stop | query (index : Nat)
  deriving DecidableEq

structure Event where
  time : Int
  op : Op

-- Half-open ranges: remove, then add, then sample at a shared timestamp.
def phase : Op → Nat | .stop => 0 | .start => 1 | .query _ => 2

def events (spans : List Span) (points : List Cell) : List Event :=
  let ends := spans.flatMap fun s => match s with
    | (some lo, some hi) => if lo < hi then [⟨lo, .start⟩, ⟨hi, .stop⟩] else []
    | _ => []
  let probes := points.zipIdx.filterMap fun (p,i) => p.map fun t => Event.mk t (.query i)
  (ends ++ probes).mergeSort fun a b =>
    if a.time == b.time then phase a.op ≤ phase b.op else a.time < b.time

def sweep (spans : List Span) (points : List Cell) : List Nat := Id.run do
  let mut active := 0
  let mut out := Array.replicate points.length 0
  for e in events spans points do
    match e.op with
    | .stop => active := active - 1
    | .start => active := active + 1
    | .query i => out := out.setIfInBounds i active
  return out.toList

def Goal : Prop := ∀ spans points, sweep spans points = reference spans points

private def spans : List Span :=
  [(some 1,some 2), (some 2,some 3), (some 2,some 3), (some 2,some 2), (none,some 4)]
private def points : List Cell := [some 2, none, some 1, some 3, some 2]
example : sweep spans points = [2,0,1,0,2] := by cbv
example : reference spans points = [2,0,1,0,2] := by decide

end TinylambOpt.ResearchGoals.IntervalCountSweep
