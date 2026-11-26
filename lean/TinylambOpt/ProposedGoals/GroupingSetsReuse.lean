import Std

/-!
One input row: two grouping columns (NULL forms its own group) and a payload.

Reference: rescan the input once for every grouping set.

Modeled claims

- `Goal` — All four grouping sets agree with a fresh scan, and the reuse path never
  reads the input more than once.
-/

namespace TinylambOpt.ProposedGoals.GroupingSetsReuse

/-- One input row: two grouping columns (NULL forms its own group) and a payload. -/
abbrev Row := Option Int × Option Int × Nat
abbrev Key := Option Int × Option Int
abbrev State := Nat × Nat -- (row count, payload sum)

def masked (ga gb : Bool) (r : Row) : Key :=
  (if ga then r.1 else none, if gb then r.2.1 else none)

def coarsen (ga gb : Bool) (k : Key) : Key :=
  (if ga then k.1 else none, if gb then k.2 else none)

def empty : State := (0, 0)

def addRow (s : State) (r : Row) : State :=
  (s.1 + 1, s.2 + r.2.2)

def combine (a b : State) : State :=
  (a.1 + b.1, a.2 + b.2)

def masks : List (Bool × Bool) :=
  [(true, true), (true, false), (false, true), (false, false)]

/-- Reference: rescan the input once for every grouping set. -/
def direct (mask : Bool × Bool) (rows : List Row) : Key → State :=
  fun k => rows.foldl (fun s r => if masked mask.1 mask.2 r == k then addRow s r else s) empty

/-- Optimized: build the finest grouping once, then fold its states into coarser groups. -/
def finest (rows : List Row) : List (Key × State) :=
  rows.foldl (fun acc r =>
    let k := masked true true r
    match acc.find? (fun p => p.1 == k) with
    | none => acc ++ [(k, addRow empty r)]
    | some (_, s) => acc.map (fun p => if p.1 == k then (k, addRow s r) else p)) []

def reuse (mask : Bool × Bool) (rows : List Row) : Key → State :=
  fun k => (finest rows).foldl
    (fun s p => if coarsen mask.1 mask.2 p.1 == k then combine s p.2 else s) empty

def directReads (rows : List Row) : Nat := masks.length * rows.length
def reuseReads (rows : List Row) : Nat := rows.length

/-- All four grouping sets agree with a fresh scan, and the reuse path never
    reads the input more than once. -/
def Goal : Prop :=
  ∀ (rows : List Row) (mask : Bool × Bool), mask ∈ masks →
    reuse mask rows = direct mask rows ∧
    (rows ≠ [] → reuseReads rows ≤ directReads rows)

example : finest [((some 1), (some 2), 5), ((some 1), none, 7), ((some 1), (some 2), 3)] =
    [((some 1, some 2), (2, 8)), ((some 1, none), (1, 7))] := by decide

example : reuse (true, false) [((some 1), (some 2), 5), ((some 1), (some 2), 3)] (some 1, none) =
    (2, 8) := by decide

end TinylambOpt.ProposedGoals.GroupingSetsReuse
