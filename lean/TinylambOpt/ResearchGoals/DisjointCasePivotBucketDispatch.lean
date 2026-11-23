import Std

namespace TinylambOpt.ResearchGoals.DisjointCasePivotBucketDispatch

inductive ElseMode where
  | elseZero
  | elseNull
  deriving DecidableEq

def combineOptInt (a b : Option Int) : Option Int :=
  match a, b with
  | none, y => y
  | x, none => x
  | some x, some y => some (x + y)

def refBucketSum {α : Type} (rows : List α) (mode : ElseMode)
    (bucket : α → Option Nat) (val : α → Option Int) (j : Nat) : Option Int :=
  rows.foldl
    (fun acc r =>
      let term :=
        if bucket r == some j then val r
        else match mode with
             | .elseZero => some 0
             | .elseNull => none
      combineOptInt acc term)
    none

def refPivotSums {α : Type} (k : Nat) (rows : List α) (mode : ElseMode)
    (bucket : α → Option Nat) (val : α → Option Int) : List (Option Int) :=
  (List.range k).map (refBucketSum rows mode bucket val)

structure SlotAcc where
  bucketHits : Nat := 0
  nonNullHits : Nat := 0
  sumVal : Int := 0
  deriving DecidableEq

def updateOneSlot (slots : List SlotAcc) (j : Nat) (v : Option Int) :
    List SlotAcc × Nat :=
  let touched := if j < slots.length then 1 else 0
  let next :=
    slots.zipIdx.map fun (s, idx) =>
      if idx == j then
        match v with
        | none => { s with bucketHits := s.bucketHits + 1 }
        | some x =>
            { bucketHits := s.bucketHits + 1
              nonNullHits := s.nonNullHits + 1
              sumVal := s.sumVal + x }
      else s
  (next, touched)

structure DispatchState where
  totalRows : Nat
  slots : List SlotAcc
  maxTouchesPerRow : Nat
  deriving DecidableEq

def finalizeSlot (mode : ElseMode) (totalRows : Nat) (s : SlotAcc) : Option Int :=
  match mode with
  | .elseNull => if s.nonNullHits == 0 then none else some s.sumVal
  | .elseZero =>
      if 0 < s.nonNullHits then some s.sumVal
      else if s.bucketHits < totalRows then some 0
      else none

/-- Single-pass disjoint bucket dispatch: evaluates `bucket r` once per row
    and updates at most one slot `j < k` instead of running `K` CASE branches. -/
def optPivotSums {α : Type} (k : Nat) (rows : List α) (mode : ElseMode)
    (bucket : α → Option Nat) (val : α → Option Int) :
    List (Option Int) × Nat :=
  let init : DispatchState :=
    { totalRows := 0
      slots := List.replicate k {}
      maxTouchesPerRow := 0 }
  let finalSt :=
    rows.foldl
      (fun st r =>
        match bucket r with
        | none =>
            { st with totalRows := st.totalRows + 1 }
        | some j =>
            let (slots', touched) := updateOneSlot st.slots j (val r)
            { totalRows := st.totalRows + 1
              slots := slots'
              maxTouchesPerRow := max st.maxTouchesPerRow touched })
      init
  (finalSt.slots.map (finalizeSlot mode finalSt.totalRows), finalSt.maxTouchesPerRow)

/-- Disjoint CASE-pivot multi-bucket dispatch (TPC-DS Q02/Q21/Q59/Q62/Q66/Q90/
    Q99): classifying each row into at most one mutually exclusive bucket `0..K-1`
    and updating only that slot matches `K` independent conditional `SUM` passes
    under both `ELSE 0` and `ELSE NULL` (including empty inputs and rows where
    a bucket matches `val = NULL`), while updating `≤ 1` slot per row. -/
def Goal : Prop :=
  ∀ {α : Type} (k : Nat) (rows : List α) (mode : ElseMode)
    (bucket : α → Option Nat) (val : α → Option Int),
    let (res, maxTouches) := optPivotSums k rows mode bucket val
    res = refPivotSums k rows mode bucket val ∧ maxTouches ≤ 1

example :
    let rows : List (Option Nat × Option Int) := [(some 0, none)]
    optPivotSums 2 rows .elseZero Prod.fst Prod.snd =
      (refPivotSums 2 rows .elseZero Prod.fst Prod.snd, 1) ∧
    (optPivotSums 2 rows .elseZero Prod.fst Prod.snd).1 = [none, some 0] ∧
    (optPivotSums 2 ([] : List (Option Nat × Option Int)) .elseZero Prod.fst Prod.snd).1 =
      [none, none] := by
  decide

example :
    let rows : List (Option Nat × Option Int) :=
      [(none, some 5), (some 1, some 10), (some 1, some 20), (some 9, some 30)]
    optPivotSums 3 rows .elseZero Prod.fst Prod.snd =
      (refPivotSums 3 rows .elseZero Prod.fst Prod.snd, 1) ∧
    optPivotSums 3 rows .elseNull Prod.fst Prod.snd =
      (refPivotSums 3 rows .elseNull Prod.fst Prod.snd, 1) := by
  decide

end TinylambOpt.ResearchGoals.DisjointCasePivotBucketDispatch
