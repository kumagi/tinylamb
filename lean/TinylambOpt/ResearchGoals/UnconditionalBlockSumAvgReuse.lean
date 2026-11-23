import Std

namespace TinylambOpt.ResearchGoals.UnconditionalBlockSumAvgReuse

structure Row where
  shipdate : Option Int
  qty : Option Int
  price : Int
  disc : Int
  tax : Int
  deriving DecidableEq

structure Block where
  minDate : Int
  maxDate : Int
  hasNullDate : Bool
  rows : List Row
  deriving DecidableEq

def ValidBlockHeader (b : Block) : Prop :=
  ∀ r ∈ b.rows,
    match r.shipdate with
    | none => b.hasNullDate = true
    | some d => b.minDate ≤ d ∧ d ≤ b.maxDate

def datePasses (cutoff : Int) : Option Int → Bool
  | none => false
  | some d => decide (d ≤ cutoff)

structure AggOut where
  sumQty : Option Int := none
  cntQty : Nat := 0
  sumDiscPrice : Option Int := none
  sumCharge : Option Int := none
  cntStar : Nat := 0
  deriving DecidableEq

def addOptInt (cur : Option Int) (v : Int) : Option Int :=
  match cur with
  | none => some v
  | some s => some (s + v)

def stepRow (acc : AggOut) (r : Row) : AggOut :=
  let (sq, cq) :=
    match r.qty with
    | none => (acc.sumQty, acc.cntQty)
    | some q => (addOptInt acc.sumQty q, acc.cntQty + 1)
  let discPrice := r.price * (100 - r.disc)
  let charge := discPrice * (100 + r.tax)
  { sumQty := sq
    cntQty := cq
    sumDiscPrice := addOptInt acc.sumDiscPrice discPrice
    sumCharge := addOptInt acc.sumCharge charge
    cntStar := acc.cntStar + 1 }

/-- Reference: evaluate `shipdate ≤ cutoff` per row across all blocks and
    compute each aggregate expression independently on matching rows. -/
def refScan (cutoff : Int) (bs : List Block) : AggOut :=
  let flat := bs.flatMap Block.rows
  let passing := flat.filter (fun r => datePasses cutoff r.shipdate)
  { sumQty :=
      passing.foldl (fun a r => match r.qty with | none => a | some q => addOptInt a q) none
    cntQty := (passing.filter (fun r => r.qty.isSome)).length
    sumDiscPrice :=
      passing.foldl (fun a r => addOptInt a (r.price * (100 - r.disc))) none
    sumCharge :=
      passing.foldl
        (fun a r => addOptInt a ((r.price * (100 - r.disc)) * (100 + r.tax))) none
    cntStar := passing.length }

structure ScanResult where
  out : AggOut
  dateChecks : Nat
  deriving DecidableEq

/-- Optimized block-skip + unconditional loop + shared `discPrice` &
    `(sumQty, cntQty)` accumulator. -/
def optScan (cutoff : Int) (bs : List Block) : ScanResult :=
  bs.foldl
    (fun (st : ScanResult) (b : Block) =>
      if cutoff < b.minDate then
        st
      else if !b.hasNullDate && b.maxDate ≤ cutoff then
        { out := b.rows.foldl stepRow st.out
          dateChecks := st.dateChecks }
      else
        let out' :=
          b.rows.foldl
            (fun acc r => if datePasses cutoff r.shipdate then stepRow acc r else acc)
            st.out
        { out := out'
          dateChecks := st.dateChecks + b.rows.length })
    { out := {}, dateChecks := 0 }

/-- Using `cntStar` instead of `cntQty` for `AVG(qty)` disagrees when `qty`
    is NULL on a passing row. -/
def badAvgUsesCountStar (cutoff : Int) (bs : List Block) : Nat :=
  (refScan cutoff bs).cntStar

/-- Block-level zone-map pruning, unconditional inner loop without per-row date
    predicate evaluation, and shared `discPrice` / `(sumQty, cntQty)` state
    (TPC-H Q01, TPC-DS Q17/Q35) match independent SQL aggregates when headers
    are valid, while executing zero date checks on fully covered or excluded
    blocks. -/
def Goal : Prop :=
  ∀ (cutoff : Int) (bs : List Block),
    (∀ b ∈ bs, ValidBlockHeader b) →
      let res := optScan cutoff bs
      res.out = refScan cutoff bs ∧
      ((∀ b ∈ bs, b.hasNullDate = false ∧ b.maxDate ≤ cutoff) →
        res.dateChecks = 0)

private def bPass : Block :=
  { minDate := 100, maxDate := 120, hasNullDate := false
    rows :=
      [ { shipdate := some 100, qty := some 10, price := 50, disc := 5, tax := 8 }
      , { shipdate := some 120, qty := none, price := 20, disc := 0, tax := 10 }
      ] }

private def bSkip : Block :=
  { minDate := 200, maxDate := 250, hasNullDate := true
    rows :=
      [ { shipdate := none, qty := some 99, price := 10, disc := 0, tax := 0 }
      , { shipdate := some 210, qty := some 88, price := 10, disc := 0, tax := 0 }
      ] }

example :
    optScan 150 [bPass, bSkip] = { out := refScan 150 [bPass, bSkip], dateChecks := 0 } ∧
    (refScan 150 [bPass, bSkip]).cntQty = 1 ∧
    badAvgUsesCountStar 150 [bPass, bSkip] = 2 := by
  decide

example :
    optScan 150 ([] : List Block) = { out := refScan 150 [], dateChecks := 0 } := by
  decide

end TinylambOpt.ResearchGoals.UnconditionalBlockSumAvgReuse
