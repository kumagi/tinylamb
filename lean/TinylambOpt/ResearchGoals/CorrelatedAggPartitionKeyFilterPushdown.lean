import Std

namespace TinylambOpt.ResearchGoals.CorrelatedAggPartitionKeyFilterPushdown

structure CteRow where
  partKey : Int
  entityKey : Int
  val : Int
  deriving DecidableEq

def partSumCnt (rows : List CteRow) (pk : Int) : Int × Nat :=
  rows.foldl
    (fun (s, c) r => if r.partKey == pk then (s + r.val, c + 1) else (s, c))
    (0, 0)

def aboveThreshold (rows : List CteRow) (num den : Nat) (r : CteRow) : Bool :=
  let (s, c) := partSumCnt rows r.partKey
  decide (Int.ofNat num * s < Int.ofNat den * Int.ofNat c * r.val)

/-- Reference correlated threshold evaluation over full CTE. -/
def refQuery (rows : List CteRow) (outerPred : CteRow → Bool) (num den : Nat) :
    List CteRow :=
  rows.filter (fun r => outerPred r && aboveThreshold rows num den r)

/-- When outer predicate depends ONLY on `partKey`, pushing it into `rows`
    before computing partition aggregates is sound. -/
def optPushPartFilter (rows : List CteRow) (pPart : Int → Bool) (num den : Nat) :
    List CteRow :=
  let filtered := rows.filter (fun r => pPart r.partKey)
  filtered.filter (aboveThreshold filtered num den)

/-- When outer predicate depends on `entityKey` (e.g. `ca_state = 'GA'` on
    `ctr_customer_sk`), semi-join prune by active `partKey`s BEFORE computing
    partition averages while keeping ALL entity rows inside each active partition. -/
def optSemiJoinActiveParts (rows : List CteRow) (pEntity : Int → Bool) (num den : Nat) :
    List CteRow :=
  let activeParts :=
    (rows.filter (fun r => pEntity r.entityKey)).map CteRow.partKey
  let narrowed := rows.filter (fun r => activeParts.contains r.partKey)
  narrowed.filter (fun r => pEntity r.entityKey && aboveThreshold narrowed num den r)

/-- Correlated partition-average filter pushdown vs semi-join partition pruning
    (TPC-DS Q01 `s_state='TN'` on `ctr_store_sk`, Q30/Q81 `ca_state='GA'` on
    `c_customer_sk`, Q65):
    (1) pushing a `partKey`-only predicate into the CTE preserves exact averages,
    (2) for an `entityKey` predicate, pruning unreferenced `partKey`s (while
    retaining all entity rows in active partitions) preserves exact averages,
    and (3) naively pushing an `entityKey` predicate changes partition averages. -/
def Goal : Prop :=
  (∀ (rows : List CteRow) (pPart : Int → Bool) (num den : Nat),
    optPushPartFilter rows pPart num den =
      refQuery rows (fun r => pPart r.partKey) num den) ∧
  (∀ (rows : List CteRow) (pEntity : Int → Bool) (num den : Nat),
    optSemiJoinActiveParts rows pEntity num den =
      refQuery rows (fun r => pEntity r.entityKey) num den) ∧
  (∃ (rows : List CteRow) (pEntity : Int → Bool) (num den : Nat),
    let wrongPush :=
      (rows.filter (fun r => pEntity r.entityKey)).filter
        (aboveThreshold (rows.filter (fun r => pEntity r.entityKey)) num den)
    wrongPush ≠ refQuery rows (fun r => pEntity r.entityKey) num den)

private def cteSample : List CteRow :=
  [ { partKey := 1, entityKey := 10, val := 100 }
  , { partKey := 1, entityKey := 20, val := 200 }
  , { partKey := 2, entityKey := 20, val := 300 }
  ]

example :
    let pPart : Int → Bool := fun pk => pk == 1
    let pEnt : Int → Bool := fun ek => ek == 10
    optPushPartFilter cteSample pPart 12 10 =
      refQuery cteSample (fun r => pPart r.partKey) 12 10 ∧
    optSemiJoinActiveParts cteSample pEnt 12 10 =
      refQuery cteSample (fun r => pEnt r.entityKey) 12 10 := by
  decide

example :
    let pEnt : Int → Bool := fun ek => ek == 10
    let wrong :=
      (cteSample.filter (fun r => pEnt r.entityKey)).filter
        (aboveThreshold (cteSample.filter (fun r => pEnt r.entityKey)) 8 10)
    wrong ≠ refQuery cteSample (fun r => pEnt r.entityKey) 8 10 := by
  decide

end TinylambOpt.ResearchGoals.CorrelatedAggPartitionKeyFilterPushdown
