import Std

namespace TinylambOpt.ResearchGoals.SinglePassMultiConsumerFactScan

structure Row where
  key : Int
  val : Option Int
  p1 : Bool
  p2 : Bool
  p3 : Bool
  deriving DecidableEq

def addUniqKey (xs : List Int) (k : Int) : List Int :=
  if xs.contains k then xs else xs ++ [k]

def addGroupSumCnt (m : List (Int × (Int × Nat))) (k : Int) (v : Int) :
    List (Int × (Int × Nat)) :=
  match m with
  | [] => [(k, (v, 1))]
  | (k', (s, c)) :: rest =>
      if k' == k then (k', (s + v, c + 1)) :: rest
      else (k', (s, c)) :: addGroupSumCnt rest k v

structure ThreeOutputs where
  distinctKeys : List Int := []
  globalSumCnt : Int × Nat := (0, 0)
  groupedSumCnt : List (Int × (Int × Nat)) := []
  reads : Nat := 0
  deriving DecidableEq

def stepC1 (xs : List Int) (r : Row) : List Int :=
  if r.p1 then addUniqKey xs r.key else xs

def stepC2 (sc : Int × Nat) (r : Row) : Int × Nat :=
  if r.p2 then
    match r.val with
    | none => sc
    | some v => (sc.1 + v, sc.2 + 1)
  else sc

def stepC3 (m : List (Int × (Int × Nat))) (r : Row) : List (Int × (Int × Nat)) :=
  if r.p3 then
    match r.val with
    | none => m
    | some v => addGroupSumCnt m r.key v
  else m

/-- Reference: 3 separate full scans for the 3 heterogeneous consumers. -/
def refThreePass (rows : List Row) : ThreeOutputs :=
  { distinctKeys := (rows.filter Row.p1).foldl stepC1 []
    globalSumCnt := (rows.filter Row.p2).foldl stepC2 (0, 0)
    groupedSumCnt := (rows.filter Row.p3).foldl stepC3 []
    reads := 3 * rows.length }

/-- Optimized single-pass union-filtered dispatch: reads each row at most once
    and updates only matching consumer states. -/
def optSinglePass (rows : List Row) : ThreeOutputs :=
  let surviving := rows.filter (fun r => r.p1 || r.p2 || r.p3)
  let init : ThreeOutputs := { reads := rows.length }
  surviving.foldl
    (fun out r =>
      { distinctKeys := stepC1 out.distinctKeys r
        globalSumCnt := stepC2 out.globalSumCnt r
        groupedSumCnt := stepC3 out.groupedSumCnt r
        reads := out.reads })
    init

/-- Multi-consumer single-pass fact scan sharing (TPC-DS Q14 `cross_items` +
    `avg_sales` + branches, Q44 dual-RANK `store_sales`, Q65 `sb`+`sc`
    `store_sales`): dispatching one scan with union predicate `p1 || p2 || p3`
    across heterogeneous consumers produces identical outputs while bounding
    row reads to `rows.length` vs `3 * rows.length`. -/
def Goal : Prop :=
  ∀ (rows : List Row),
    let ref := refThreePass rows
    let opt := optSinglePass rows
    opt.distinctKeys = ref.distinctKeys ∧
    opt.globalSumCnt = ref.globalSumCnt ∧
    opt.groupedSumCnt = ref.groupedSumCnt ∧
    opt.reads ≤ rows.length ∧
    (0 < rows.length → opt.reads < ref.reads)

private def sampleFact : List Row :=
  [ { key := 10, val := some 5, p1 := true, p2 := true, p3 := true }
  , { key := 10, val := none, p1 := true, p2 := true, p3 := false }
  , { key := 20, val := some 7, p1 := false, p2 := false, p3 := false }
  , { key := 30, val := some 9, p1 := false, p2 := true, p3 := true }
  ]

example :
    let ref := refThreePass sampleFact
    let opt := optSinglePass sampleFact
    opt.distinctKeys = ref.distinctKeys ∧
    opt.globalSumCnt = ref.globalSumCnt ∧
    opt.groupedSumCnt = ref.groupedSumCnt ∧
    opt.reads = 4 ∧ ref.reads = 12 := by
  decide

example :
    let opt := optSinglePass []
    opt.distinctKeys = [] ∧ opt.globalSumCnt = (0, 0) ∧ opt.groupedSumCnt = [] := by
  decide

end TinylambOpt.ResearchGoals.SinglePassMultiConsumerFactScan
