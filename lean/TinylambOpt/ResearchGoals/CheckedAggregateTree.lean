import TinylambOpt.CheckedEagerSum

/-!
Input for one group in execution order. Its original ordinals can be sparse.

Build summaries once before evaluation. The evaluator can inspect the root summary
without traversing the underlying rows.

Modeled claims

- `Goal` — Prove against the same independently specified scalar scan for every
  bracketing and valid incoming state. Safe inputs must take the summary path.
-/

namespace TinylambOpt.ResearchGoals.CheckedAggregateTree

open TinylambOpt.CheckedEagerSum (InRange)

/-- Input for one group in execution order. Its original ordinals can be sparse. -/
structure Row where
  ordinal : Nat
  value : Option Int64
  deriving DecidableEq, Repr

abbrev Result := Except Nat (Option Int)

def add (acc : Option Int) (v : Option Int64) : Option Int :=
  match v with
  | none => acc
  | some x => some (acc.getD 0 + x.toInt)

def sequential (acc : Option Int) : List Row → Result
  | [] => .ok acc
  | row :: rows =>
    let next := add acc row.value
    if InRange (next.getD 0) then sequential next rows else .error row.ordinal

inductive Tree where
  | chunk (rows : List Row)
  | join (left right : Tree)

def Tree.rows : Tree → List Row
  | .chunk xs => xs
  | .join l r => l.rows ++ r.rows

structure Summary where
  total : Int
  low : Int
  high : Int
  count : Nat

def combine (l r : Summary) : Summary :=
  ⟨l.total + r.total, min l.low (l.total + r.low),
    max l.high (l.total + r.high), l.count + r.count⟩

def rowSummary (r : Row) : Summary :=
  match r.value with
  | none => ⟨0,0,0,0⟩
  | some v => ⟨v.toInt,min 0 v.toInt,max 0 v.toInt,1⟩

/-- Build summaries once before evaluation. The evaluator can inspect the root
    summary without traversing the underlying rows. -/
inductive Cached where
  | chunk (summary : Summary) (rows : List Row)
  | join (summary : Summary) (left right : Cached)

def Cached.summary : Cached → Summary
  | .chunk s _ | .join s _ _ => s

def prepare : Tree → Cached
  | .chunk rows => .chunk (rows.foldl (fun s r => combine s (rowSummary r)) ⟨0,0,0,0⟩) rows
  | .join l r =>
    let left := prepare l
    let right := prepare r
    .join (combine left.summary right.summary) left right

/-- Log the query-time operations. A summary hit includes the empty/all-NULL case. -/
inductive Event where
  | skip | openChunk | descend
  deriving DecidableEq, Repr

def run (acc : Option Int) (tree : Cached) : Result × List Event :=
  let s := tree.summary
  let a := acc.getD 0
  if InRange (a + s.low) ∧ InRange (a + s.high) then
    (.ok (if s.count = 0 then acc else some (a+s.total)), [.skip])
  else match tree with
    | .chunk _ rows => (sequential acc rows, [.openChunk])
    | .join _ l r =>
      let (res, trace) := run acc l
      match res with
      | .error i => (.error i, .descend :: trace)
      | .ok b =>
        let (out, rest) := run b r
        (out, .descend :: (trace ++ rest))

/-- Prove against the same independently specified scalar scan for every
    bracketing and valid incoming state. Safe inputs must take the summary path. -/
def Goal : Prop :=
  ∀ (tree : Tree) (acc : Option Int), InRange (acc.getD 0) →
    (run acc (prepare tree)).1 = sequential acc tree.rows ∧
    ((run acc (prepare tree)).2 = [.skip] ↔
      ∀ n ≤ tree.rows.length, InRange (((tree.rows.take n).foldl
        (fun a row => add a row.value) acc).getD 0))

/-- Contract fixtures fix error ordinals, NULL, and genuine zero. -/
example : sequential none [⟨7,some 9223372036854775807⟩,⟨41,some 1⟩,⟨90,some (-1)⟩] =
    .error 41 := by rfl
example : (run none (prepare (.chunk [⟨7,none⟩]))).1 = .ok none ∧
    (run none (prepare (.chunk [⟨7,some 0⟩]))) = (.ok (some 0),[.skip]) := by constructor <;> rfl

end TinylambOpt.ResearchGoals.CheckedAggregateTree
