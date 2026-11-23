import Std

namespace TinylambOpt.ResearchGoals.SelectiveFilterLateSliceGroup

structure RawRow where
  intColsPass : Bool
  strNonEmpty : Bool
  strVal : String
  useRef : Bool
  refVal : String
  deriving DecidableEq

abbrev GroupKey := String × String

def addGroupCnt (m : List (GroupKey × Nat)) (k : GroupKey) :
    List (GroupKey × Nat) :=
  match m with
  | [] => [(k, 1)]
  | (k', c) :: rest =>
      if k' == k then (k', c + 1) :: rest
      else (k', c) :: addGroupCnt rest k

def refFilterGroup (rows : List RawRow) (checkNonEmptyStr : Bool) :
    List (GroupKey × Nat) :=
  rows.foldl
    (fun acc r =>
      let okStr := if checkNonEmptyStr then r.strVal != "" else true
      if r.intColsPass && okStr then
        let src := if r.useRef then r.refVal else ""
        addGroupCnt acc (r.strVal, src)
      else acc)
    []

structure TraceOut where
  groups : List (GroupKey × Nat)
  intMatched : Nat
  strSliceDecodes : Nat
  refSliceDecodes : Nat
  deriving DecidableEq

def ValidRowLengths (r : RawRow) : Prop :=
  r.strNonEmpty = (r.strVal != "")

/-- Late offset-slice string group aggregation (ClickBench Q37/Q38/Q40):
    evaluate cheap integer filters to build sparse selection vector `sel`,
    screen `len > 0` from offset array before slicing strings, and slice/decode
    `refVal` ONLY on surviving rows where `useRef = true`. -/
def optLateSliceGroup (rows : List RawRow) (checkNonEmptyStr : Bool) : TraceOut :=
  let sel := rows.filter RawRow.intColsPass
  sel.foldl
    (fun st r =>
      let passStr := if checkNonEmptyStr then r.strNonEmpty else true
      if !passStr then st
      else
        let src := if r.useRef then r.refVal else ""
        { groups := addGroupCnt st.groups (r.strVal, src)
          intMatched := st.intMatched
          strSliceDecodes := st.strSliceDecodes + 1
          refSliceDecodes := st.refSliceDecodes + (if r.useRef then 1 else 0) })
    { groups := [], intMatched := sel.length, strSliceDecodes := 0, refSliceDecodes := 0 }

/-- Selective integer pre-filter + lazy VarChar slice group-by + guarded
    `CASE` string decode (ClickBench Q37/Q38/Q40): delaying string slicing past
    integer selection and length checks while guarding secondary string decodes
    under `CASE WHEN cond THEN ref ELSE ""` produces exact group counts with
    zero decodes on filtered-out or constant-branch rows. -/
def Goal : Prop :=
  ∀ (rows : List RawRow) (checkNonEmptyStr : Bool),
    (∀ r ∈ rows, ValidRowLengths r) →
      let tr := optLateSliceGroup rows checkNonEmptyStr
      tr.groups = refFilterGroup rows checkNonEmptyStr ∧
      tr.strSliceDecodes ≤ tr.intMatched ∧
      tr.refSliceDecodes ≤ tr.strSliceDecodes ∧
      ((∀ r ∈ rows, r.intColsPass = true → r.useRef = false) → tr.refSliceDecodes = 0)

private def sampleBlock : List RawRow :=
  [ { intColsPass := false, strNonEmpty := true, strVal := "a", useRef := true, refVal := "r1" }
  , { intColsPass := true, strNonEmpty := false, strVal := "", useRef := true, refVal := "r2" }
  , { intColsPass := true, strNonEmpty := true, strVal := "u1", useRef := false, refVal := "r3" }
  , { intColsPass := true, strNonEmpty := true, strVal := "u1", useRef := false, refVal := "r4" }
  ]

example :
    let tr := optLateSliceGroup sampleBlock true
    tr.groups = refFilterGroup sampleBlock true ∧
    tr.intMatched = 3 ∧ tr.strSliceDecodes = 2 ∧ tr.refSliceDecodes = 0 := by
  decide

example :
    let tr := optLateSliceGroup sampleBlock false
    tr.groups = refFilterGroup sampleBlock false ∧
    tr.strSliceDecodes = 3 ∧ tr.refSliceDecodes = 1 := by
  decide

end TinylambOpt.ResearchGoals.SelectiveFilterLateSliceGroup
