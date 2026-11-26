import Std

/-!
Stale block-lazy header example where `maxKey` underreports the rows, causing
`zmRowUpperBound` to undercount actual surviving rows.

Modeled claims

- `Goal` — Immutable TLCS v3 zone-map row-count upper bound (TPC-DS Q22 1-pass ROLLUP
  `exact_integer_total` restore): when blocks carry immutable valid headers, summing
  row counts of zone-map-matching blocks upper-bounds surviving probe rows, so
  `zmRows * buildFanout * maxAbs ≤ exactLimit` guarantees `(survivingRows *
  buildFanout) * maxAbs ≤ exactLimit`.
-/

namespace TinylambOpt.ResearchGoals.ImmutableTlcsZoneMapRowBound

structure Block where
  minKey : Int
  maxKey : Int
  hasNull : Bool
  allNull : Bool
  rows : List (Option Int × Int)
  deriving DecidableEq

def ValidBlockHeader (b : Block) : Prop :=
  ∀ r ∈ b.rows,
    match r.1 with
    | none => b.hasNull = true
    | some k => b.allNull = false ∧ b.minKey ≤ k ∧ k ≤ b.maxKey

def inFilter (lo hi : Int) : Option Int → Bool
  | none => false
  | some k => decide (lo ≤ k ∧ k ≤ hi)

def survivingRowCount (lo hi : Int) (bs : List Block) : Nat :=
  ((bs.flatMap Block.rows).filter (fun r => inFilter lo hi r.1)).length

def mayMatchHeader (lo hi : Int) (b : Block) : Bool :=
  decide (lo ≤ hi ∧ !b.allNull ∧ b.minKey ≤ hi ∧ lo ≤ b.maxKey)

def zmRowUpperBound (lo hi : Int) (bs : List Block) : Nat :=
  ((bs.filter (mayMatchHeader lo hi)).map (fun b => b.rows.length)).sum

def withinExactSumLimit (rowCount buildFanout maxAbs exactLimit : Nat) : Prop :=
  rowCount * buildFanout * maxAbs ≤ exactLimit

/-- Stale block-lazy header example where `maxKey` underreports the rows,
    causing `zmRowUpperBound` to undercount actual surviving rows. -/
def staleHeaderExample : Block :=
  { minKey := 1, maxKey := 5, hasNull := false, allNull := false
    rows := [(some 20, 100), (some 25, 200)] }

/-- Immutable TLCS v3 zone-map row-count upper bound (TPC-DS Q22 1-pass ROLLUP
    `exact_integer_total` restore): when blocks carry immutable valid headers,
    summing row counts of zone-map-matching blocks upper-bounds surviving probe
    rows, so `zmRows * buildFanout * maxAbs ≤ exactLimit` guarantees
    `(survivingRows * buildFanout) * maxAbs ≤ exactLimit`. -/
def Goal : Prop :=
  (∀ (lo hi : Int) (bs : List Block) (buildFanout maxAbs exactLimit : Nat),
    (∀ b ∈ bs, ValidBlockHeader b) →
      survivingRowCount lo hi bs ≤ zmRowUpperBound lo hi bs ∧
      (withinExactSumLimit (zmRowUpperBound lo hi bs) buildFanout maxAbs exactLimit →
        withinExactSumLimit (survivingRowCount lo hi bs) buildFanout maxAbs exactLimit)) ∧
  (zmRowUpperBound 10 30 [staleHeaderExample] < survivingRowCount 10 30 [staleHeaderExample])

private def b1 : Block :=
  { minKey := 1, maxKey := 5, hasNull := false, allNull := false, rows := [(some 1, 1), (some 5, 2)] }
private def b2 : Block :=
  { minKey := 10, maxKey := 20, hasNull := true, allNull := false, rows := [(none, 3), (some 15, 4)] }
private def b3 : Block :=
  { minKey := 0, maxKey := 0, hasNull := true, allNull := true, rows := [(none, 5), (none, 6)] }

example :
    survivingRowCount 10 20 [b1, b2, b3] = 1 ∧
    zmRowUpperBound 10 20 [b1, b2, b3] = 2 ∧
    zmRowUpperBound 10 30 [staleHeaderExample] < survivingRowCount 10 30 [staleHeaderExample] := by
  decide

example :
    zmRowUpperBound 20 10 [b1, b2, b3] = 0 ∧ survivingRowCount 20 10 [b1, b2, b3] = 0 := by
  decide

end TinylambOpt.ResearchGoals.ImmutableTlcsZoneMapRowBound
