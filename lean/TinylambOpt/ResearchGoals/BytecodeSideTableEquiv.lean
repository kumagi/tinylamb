import Std

namespace TinylambOpt.ResearchGoals.BytecodeSideTableEquiv

inductive Inst where
  | loadConst (idx : Nat)
  | loadIntCol (col : Nat)
  | inInt64Range (specIdx : Nat)
  | divConst (specIdx : Nat)
  | inStringSet (strCol : Nat) (specIdx : Nat)
  | add
  deriving DecidableEq

structure Program where
  insts : List Inst
  consts : List Int
  ranges : List (Int × Int)
  recips : List Int
  stringSets : List (List String)
  resultTypeTag : Nat
  deriving DecidableEq

structure DataRow where
  ints : List Int
  strs : List String
  deriving DecidableEq

def evalStep (p : Program) (row : DataRow) (stk : List Int) : Inst → List Int
  | .loadConst idx => (p.consts[idx]?.getD 0) :: stk
  | .loadIntCol col => (row.ints[col]?.getD 0) :: stk
  | .inInt64Range specIdx =>
      match stk with
      | x :: rest =>
          match p.ranges[specIdx]? with
          | some (lo, hi) => (if decide (lo ≤ x ∧ x ≤ hi) then 1 else 0) :: rest
          | none => 0 :: rest
      | [] => []
  | .divConst specIdx =>
      match stk with
      | x :: rest =>
          match p.recips[specIdx]? with
          | some d => (if d == 0 then 0 else x / d) :: rest
          | none => 0 :: rest
      | [] => []
  | .inStringSet strCol specIdx =>
      let s := row.strs[strCol]?.getD ""
      match p.stringSets[specIdx]? with
      | some ss => (if ss.contains s then 1 else 0) :: stk
      | none => 0 :: stk
  | .add =>
      match stk with
      | b :: a :: rest => (a + b) :: rest
      | _ => stk

def evalProg (p : Program) (row : DataRow) : Int :=
  (p.insts.foldl (evalStep p row) []).headD 0

def evalRefSlots (progs : List Program) (rows : List DataRow) : List Int :=
  progs.map fun p => (rows.map (evalProg p)).sum

def assignSlots (same : Program → Program → Bool) (progs : List Program) : List Nat :=
  let rec go (seen : List (Program × Nat)) : List Program → List Nat
    | [] => []
    | p :: rest =>
        match seen.find? (fun pair => same pair.1 p) with
        | some (_, slotIdx) => slotIdx :: go seen rest
        | none =>
            let newSlot := seen.length
            newSlot :: go (seen ++ [(p, newSlot)]) rest
  go [] progs

def evalSlotsWith (same : Program → Program → Bool) (progs : List Program)
    (rows : List DataRow) : List Int :=
  let slotIds := assignSlots same progs
  let uniqueProgs :=
    (progs.zip slotIds).foldl
      (fun acc (p, sid) =>
        if acc.any (fun q => q.1 == sid) then acc else acc ++ [(sid, p)])
      ([] : List (Nat × Program))
  let slotSums := uniqueProgs.map fun (sid, p) => (sid, (rows.map (evalProg p)).sum)
  slotIds.map fun sid =>
    match slotSums.find? (fun q => q.1 == sid) with
    | some (_, s) => s
    | none => 0

def sameFullProgram (a b : Program) : Bool :=
  decide (a = b)

/-- Stack-VM bytecode CSE / aggregate slot deduplication
    (`executor/detail/fused_grouped_join.cpp` `same_child_program`): slot reuse
    is sound when and only when side tables (`ranges`, `recips`, `stringSets`,
    `resultTypeTag`) are compared alongside `insts` and `consts`. -/
def Goal : Prop :=
  ∀ (progs : List Program) (rows : List DataRow),
    evalSlotsWith sameFullProgram progs rows = evalRefSlots progs rows

/-- Buggy `same_child_program` comparing only `insts`, `consts`, and
    `resultTypeTag` while ignoring `ranges`/`recips`/`stringSets` (the TPC-DS Q90
    aliasing bug). -/
def badSameInstsConstsOnly (a b : Program) : Bool :=
  a.insts == b.insts && a.consts == b.consts && a.resultTypeTag == b.resultTypeTag

private def q90Amc : Program :=
  ⟨[.loadIntCol 0, .inInt64Range 0], [], [(8, 9)], [], [], 1⟩
private def q90Pmc : Program :=
  ⟨[.loadIntCol 0, .inInt64Range 0], [], [(19, 20)], [], [], 1⟩
private def q90Rows : List DataRow :=
  [⟨[8], []⟩, ⟨[9], []⟩, ⟨[19], []⟩, ⟨[12], []⟩]

example :
    evalSlotsWith sameFullProgram [q90Amc, q90Pmc] q90Rows = [2, 1] ∧
    evalRefSlots [q90Amc, q90Pmc] q90Rows = [2, 1] := by
  decide

example :
    evalSlotsWith badSameInstsConstsOnly [q90Amc, q90Pmc] q90Rows = [2, 2] := by
  decide

end TinylambOpt.ResearchGoals.BytecodeSideTableEquiv
