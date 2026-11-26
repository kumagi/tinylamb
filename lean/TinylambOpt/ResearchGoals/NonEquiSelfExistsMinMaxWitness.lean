import Std

/-!
Reference SQL 3VL self-EXISTS `r2.oid = r.oid ∧ r2.wh <> r.wh`.

O(1) per-group witness check using precomputed `(minWh, maxWh)` without self-joining on
`cs_order_number`.

Modeled claims

- `Goal` — Group `(min, max)` witness reduction for non-equi self-`EXISTS` (TPC-DS Q16
  `EXISTS (SELECT * FROM catalog_sales cs2 WHERE cs1.oid = cs2.oid AND cs1.wh <>
  cs2.wh)`): checking `minWh < maxWh ∨ wh ≠ minWh` against the group summary matches
  quadratic self-join lookup (returning `false` whenever `wh` is `NULL`).
-/

namespace TinylambOpt.ResearchGoals.NonEquiSelfExistsMinMaxWitness

abbrev Row := Int × Option Int  -- (order_number, warehouse_sk)

/-- Reference SQL 3VL self-EXISTS `r2.oid = r.oid ∧ r2.wh <> r.wh`. -/
def refSelfExists (rows : List Row) (r : Row) : Bool :=
  rows.any fun r2 =>
    r2.1 == r.1 &&
      match r.2, r2.2 with
      | some a, some b => a != b
      | _, _ => false

def groupMinMax (rows : List Row) (oid : Int) : Option (Int × Int) :=
  rows.foldl
    (fun acc r2 =>
      if r2.1 != oid then acc
      else
        match r2.2, acc with
        | none, _ => acc
        | some b, none => some (b, b)
        | some b, some (mn, mx) => some (min mn b, max mx b))
    none

/-- O(1) per-group witness check using precomputed `(minWh, maxWh)` without
    self-joining on `cs_order_number`. -/
def optMinMaxWitness (rows : List Row) (r : Row) : Bool :=
  match r.2, groupMinMax rows r.1 with
  | some a, some (mn, mx) => decide (mn < mx || a != mn)
  | _, _ => false

/-- Group `(min, max)` witness reduction for non-equi self-`EXISTS` (TPC-DS
    Q16 `EXISTS (SELECT * FROM catalog_sales cs2 WHERE cs1.oid = cs2.oid AND
    cs1.wh <> cs2.wh)`): checking `minWh < maxWh ∨ wh ≠ minWh` against the
    group summary matches quadratic self-join lookup (returning `false`
    whenever `wh` is `NULL`). -/
def Goal : Prop :=
  ∀ (rows : List Row) (r : Row),
    optMinMaxWitness rows r = refSelfExists rows r

private def sampleOrders : List Row :=
  [ (1, some 5), (1, none), (1, some 5)
  , (2, some 3), (2, some 8), (2, none)
  , (3, none)
  ]

example :
    optMinMaxWitness sampleOrders (1, some 5) = false ∧
    optMinMaxWitness sampleOrders (2, none) = false ∧
    optMinMaxWitness sampleOrders (2, some 3) = true ∧
    sampleOrders.all (fun r => optMinMaxWitness sampleOrders r == refSelfExists sampleOrders r) =
      true := by
  decide

example :
    optMinMaxWitness ([] : List Row) (1, some 5) = refSelfExists [] (1, some 5) ∧
    optMinMaxWitness sampleOrders (1, some 7) = refSelfExists sampleOrders (1, some 7) := by
  decide

end TinylambOpt.ResearchGoals.NonEquiSelfExistsMinMaxWitness
