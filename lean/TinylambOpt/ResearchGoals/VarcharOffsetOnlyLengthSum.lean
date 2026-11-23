import Std

namespace TinylambOpt.ResearchGoals.VarcharOffsetOnlyLengthSum

structure VarcharBlock where
  rows : List (Option String)
  nulls : List Bool
  ends : List Nat
  deriving DecidableEq

def prevEndAt (ends : List Nat) (i : Nat) : Nat :=
  if i == 0 then 0 else ends.getD (i - 1) 0

def ValidVarcharBlock (b : VarcharBlock) : Prop :=
  b.rows.length = b.ends.length ∧
  b.nulls = b.rows.map Option.isNone ∧
  ∀ i : Nat, i < b.rows.length →
    let pe := prevEndAt b.ends i
    let e := b.ends.getD i 0
    pe ≤ e ∧
    match b.rows.getD i none with
    | none => e = pe
    | some s => e - pe = s.length

def refNonEmptyLenSum (rows : List (Option String)) : Nat :=
  (rows.filterMap fun | some s => if s != "" then some s.length else none | none => none).sum

def refNonEmptyCount (rows : List (Option String)) : Nat :=
  (rows.filter fun | some s => s != "" | none => false).length

/-- O(1) block length sum: reads ONLY the last offset without scanning any
    payload bytes or individual offsets. -/
def optO1EndsLastLenSum (b : VarcharBlock) : Nat :=
  if b.ends.isEmpty then 0 else b.ends.getLast!

def optOffsetOnlyNonEmptyCount (b : VarcharBlock) : Nat :=
  (List.range b.ends.length).foldl
    (fun acc i =>
      if !b.nulls.getD i true && prevEndAt b.ends i < b.ends.getD i 0 then acc + 1 else acc)
    0

/-- Counterexample: if a NULL row has a non-zero offset span (`ends = [5, 5]`),
    `ends.getLast!` overcounts actual non-NULL string lengths. -/
def badNullNonzeroSpan : VarcharBlock :=
  { rows := [none, some ""], nulls := [true, false], ends := [5, 5] }

/-- Offset-only `BYTE_LENGTH` & non-empty count on VarChar blocks (ClickBench
    Q28): when a block satisfies `ValidVarcharBlock` (monotone end-offsets with
    zero-length NULL spans), `∑_{URL≠""} BYTE_LENGTH(URL)` equals
    `ends.getLast!` in `O(1)` time and `O(1)` byte reads, and `COUNT(*)` reads
    only `(nulls, ends)` without touching string payload bytes. -/
def Goal : Prop :=
  (∀ b : VarcharBlock, ValidVarcharBlock b →
    optO1EndsLastLenSum b = refNonEmptyLenSum b.rows ∧
    optOffsetOnlyNonEmptyCount b = refNonEmptyCount b.rows) ∧
  (optO1EndsLastLenSum badNullNonzeroSpan ≠ refNonEmptyLenSum badNullNonzeroSpan.rows)

example :
    let b : VarcharBlock :=
      { rows := [some "abc", none, some "", some "hello"]
        nulls := [false, true, false, false]
        ends := [3, 3, 3, 8] }
    optO1EndsLastLenSum b = refNonEmptyLenSum b.rows ∧
    optOffsetOnlyNonEmptyCount b = refNonEmptyCount b.rows ∧
    optO1EndsLastLenSum b = 8 := by
  decide

example :
    optO1EndsLastLenSum badNullNonzeroSpan = 5 ∧
    refNonEmptyLenSum badNullNonzeroSpan.rows = 0 ∧
    optO1EndsLastLenSum { rows := [], nulls := [], ends := [] } = 0 := by
  decide

end TinylambOpt.ResearchGoals.VarcharOffsetOnlyLengthSum
