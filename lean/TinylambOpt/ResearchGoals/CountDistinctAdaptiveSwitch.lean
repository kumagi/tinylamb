import Std

namespace TinylambOpt.ResearchGoals.CountDistinctAdaptiveSwitch

inductive State where
  | empty
  | single (v : Int)
  | small (xs : List Int)
  | large (xs : List Int)
  deriving DecidableEq

def insertNonNull (thresh : Nat) (st : State) (v : Int) : State :=
  match st with
  | .empty => .single v
  | .single u =>
      if u == v then .single u
      else if 2 ≤ thresh then .small [v, u]
      else .large [v, u]
  | .small xs =>
      if xs.contains v then .small xs
      else
        let xs' := v :: xs
        if xs'.length ≤ thresh then .small xs' else .large xs'
  | .large xs =>
      if xs.contains v then .large xs
      else .large (v :: xs)

def insertOpt (thresh : Nat) (st : State) : Option Int → State
  | none => st
  | some v => insertNonNull thresh st v

def entriesOf : State → List Int
  | .empty => []
  | .single v => [v]
  | .small xs => xs
  | .large xs => xs

def mergeState (thresh : Nat) (s1 s2 : State) : State :=
  (entriesOf s2).foldl (insertNonNull thresh) s1

def finalize : State → Nat
  | .empty => 0
  | .single _ => 1
  | .small xs => xs.length
  | .large xs => xs.length

def StateInvariant (thresh : Nat) : State → Prop
  | .empty => True
  | .single _ => True
  | .small xs => xs.length ≤ thresh ∧ xs.Nodup
  | .large xs => xs.Nodup

def refCountDistinct (rows : List (Option Int)) : Nat :=
  (rows.filterMap id).eraseDups.length

/-- Adaptive 4-state `COUNT(DISTINCT)` (`empty`/`single`/`small`/`large`)
    (`executor/detail/typed_stream_aggregate_compact.cpp`,
    `executor/detail/aggregate_accumulator.cpp`): ignoring `none` (SQL NULL) and
    promoting through `empty -> single -> small -> large` across worker fold +
    tree merge preserves exact distinct cardinality and invariants. -/
def Goal : Prop :=
  ∀ (thresh : Nat) (chunks : List (List (Option Int))),
    1 ≤ thresh →
    let workerStates := chunks.map (fun c => c.foldl (insertOpt thresh) .empty)
    let merged := workerStates.foldl (mergeState thresh) .empty
    (∀ s ∈ workerStates, StateInvariant thresh s) ∧
    StateInvariant thresh merged ∧
    finalize merged = refCountDistinct chunks.flatten

/-- Buggy NULL handling that encodes SQL NULL as `0` inflates `COUNT(DISTINCT)`
    when `0` is absent. -/
def badInsertNullAsZero (thresh : Nat) (st : State) : Option Int → State
  | none => insertNonNull thresh st 0
  | some v => insertNonNull thresh st v

private def c1 : List (Option Int) := [none, some 5, some 5]
private def c2 : List (Option Int) := [some 7, none, some 9, some 5]

example :
    let ws := [c1, c2].map (fun c => c.foldl (insertOpt 2) .empty)
    let m := ws.foldl (mergeState 2) .empty
    m = .large [7, 9, 5] ∧ finalize m = refCountDistinct (c1 ++ c2) := by
  decide

example :
    let mBad := (c1 ++ c2).foldl (badInsertNullAsZero 2) .empty
    finalize mBad = 4 ∧ refCountDistinct (c1 ++ c2) = 3 := by
  decide

end TinylambOpt.ResearchGoals.CountDistinctAdaptiveSwitch
