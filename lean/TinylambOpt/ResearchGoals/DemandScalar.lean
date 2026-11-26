import Std

/-!
These claims describe when an execution path actually demands a scalar and its error.
SQL evaluation can defer computing that scalar only while preserving the observed
results.

Modeled claims

- `Goal` — `∀ inner outer, probe (index inner) outer = reference inner 0 outer`
-/

namespace TinylambOpt.ResearchGoals.DemandScalar

abbrev Cell := Option Int
abbrev Inner := List (Cell × Cell)
-- The flag is the evaluated CASE guard. Inactive rows project SQL NULL.
abbrev Outer := List (Bool × Cell)

inductive Answer where
  | done (values : List Cell)
  | failed (emitted : List Cell) (ordinal : Nat)
  deriving DecidableEq

inductive Hit where
  | one (value : Cell)
  | many
  deriving DecidableEq

def reference (inner : Inner) : Nat → Outer → Answer
  | _, [] => .done []
  | i, (active, key) :: tail => Id.run do
    let hits := inner.filter (fun r => active && key.isSome && r.1 == key)
    if hits.length > 1 then return .failed [] i
    let value := hits.head?.bind Prod.snd
    match reference inner (i+1) tail with
    | .done vs => return .done (value :: vs)
    | .failed vs j => return .failed (value :: vs) j

def index (inner : Inner) : Std.HashMap Int Hit :=
  inner.foldl (fun m (key, v) => match key with
    | none => m
    | some k => m.insert k (if m.contains k then .many else .one v)) {}

def probe (m : Std.HashMap Int Hit) (outer : Outer) : Answer := Id.run do
  let mut emitted : Array Cell := #[]
  for (active, key) in outer do
    let hit := if active then key.bind (fun k => m[k]?) else none
    match hit with
    | some .many => return .failed emitted.toList emitted.size
    | some (.one v) => emitted := emitted.push v
    | none => emitted := emitted.push none
  return .done emitted.toList

def Goal : Prop := ∀ inner outer, probe (index inner) outer = reference inner 0 outer

private def witness : Inner := [(some 1, none), (some 1, none), (none, some 9), (some 2, some 7)]
private def requests : Outer := [(false, some 1), (true, none), (true, some 2), (true, some 1)]
example : probe (index witness) requests = .failed [none, none, some 7] 3 := by cbv
example : reference witness 0 requests = .failed [none, none, some 7] 3 := by cbv

end TinylambOpt.ResearchGoals.DemandScalar
