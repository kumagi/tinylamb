import Std

namespace TinylambOpt.ResearchGoals.SelfJoinSharedScanDualFilterSplit

structure BaseRow where
  id : Int
  payload : Int
  cond1 : Bool
  cond2 : Bool
  deriving DecidableEq

structure Edge where
  src : Int
  dst : Int
  deriving DecidableEq

/-- Reference execution: two separate scans of `T` (total `2 * T.length` base-row
    touches), followed by joining edges `(e.src = r1.id ∧ e.dst = r2.id)`. -/
def refDualScanJoin (rows : List BaseRow) (edges : List Edge) :
    List (Int × Int) × Nat :=
  let side1 := rows.filter (fun r => r.cond1)
  let side2 := rows.filter (fun r => r.cond2)
  let out :=
    edges.flatMap fun e =>
      side1.flatMap fun r1 =>
        if r1.id == e.src then
          side2.filterMap fun r2 =>
            if r2.id == e.dst then some (r1.payload, r2.payload) else none
        else []
  (out, 2 * rows.length)

structure TaggedNode where
  id : Int
  payload : Int
  mask1 : Bool
  mask2 : Bool
  deriving DecidableEq

/-- Single-pass shared scan + dual role-bitmask split: scans `T` once
    (`T.length` row touches), tags each row with `(cond1, cond2)` role bits, and
    resolves edge joins without a second table scan. -/
def optSharedScanJoin (rows : List BaseRow) (edges : List Edge) :
    List (Int × Int) × Nat :=
  let tagged :=
    rows.map fun r =>
      { id := r.id, payload := r.payload, mask1 := r.cond1, mask2 := r.cond2 : TaggedNode }
  let out :=
    edges.flatMap fun e =>
      tagged.flatMap fun r1 =>
        if r1.mask1 && r1.id == e.src then
          tagged.filterMap fun r2 =>
            if r2.mask2 && r2.id == e.dst then some (r1.payload, r2.payload) else none
        else []
  (out, rows.length)

/-- Self-join shared base scan with dual role bitmasks (LDBC L1 `person p / p2`
    / STATS-CEB C2 `users u / u2`): building a single tagged node table with
    2 role filter bits `(cond1, cond2)` in one pass over `T` yields the exact
    same join pairs as two independent base scans for every table and edge set,
    while halving base-row scan touches whenever `T ≠ []`. -/
def Goal : Prop :=
  ∀ (rows : List BaseRow) (edges : List Edge),
    let (outOpt, costOpt) := optSharedScanJoin rows edges
    let (outRef, costRef) := refDualScanJoin rows edges
    (∀ p : Int × Int, outOpt.count p = outRef.count p) ∧
    (rows ≠ [] → costOpt < costRef)

example :
    let rows : List BaseRow :=
      [ { id := 1, payload := 100, cond1 := true, cond2 := true }
      , { id := 2, payload := 200, cond1 := true, cond2 := false }
      , { id := 3, payload := 300, cond1 := false, cond2 := true }
      , { id := 4, payload := 400, cond1 := false, cond2 := false }
      ]
    let edges : List Edge :=
      [ { src := 1, dst := 3 }
      , { src := 1, dst := 2 } -- dst fails cond2
      , { src := 2, dst := 1 } -- both match via overlap cond1/cond2
      , { src := 99, dst := 1 }
      ]
    (optSharedScanJoin rows edges).1 = (refDualScanJoin rows edges).1 ∧
    (optSharedScanJoin rows edges).1 = [(100, 300), (200, 100)] ∧
    (optSharedScanJoin rows edges).2 < (refDualScanJoin rows edges).2 := by
  decide

example :
    optSharedScanJoin [] [{ src := 1, dst := 1 }] = ([], 0) ∧
    refDualScanJoin [] [{ src := 1, dst := 1 }] = ([], 0) := by
  decide

end TinylambOpt.ResearchGoals.SelfJoinSharedScanDualFilterSplit
