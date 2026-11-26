import Std

/-!
Reference `LEFT OUTER JOIN` aggregation `(COUNT(*), COUNT(L.v), SUM(L.v))` over
`σ_pred(L) ⟕_{L.k = R.k} R` when `R` columns are unreferenced.

Optimized right-multiplicity weight collapse: counts right multiplicities on non-NULL
keys only, scans `σ_pred(L)` once, and weights each surviving left row by `w = 1` (if `k
IS NULL`) or `max 1 mult(k)`.

Modeled claims

- `Goal` — Right-side multiplicity collapse for `LEFT OUTER JOIN` without right-column
  references (STATS-CEB C4): replacing materialization of `R` matches with
  per-non-NULL-key multiplicity weights `max 1 mult(k)` (and `1` for NULL join keys)
  yields identical `(COUNT(*), COUNT(L.v), SUM(L.v))`.
-/

namespace TinylambOpt.ResearchGoals.LeftJoinRightMultiplicityCollapse

structure LRow where
  k : Option String
  v : Option Int
  pred : Bool
  deriving DecidableEq

structure RRow where
  k : Option String
  deriving DecidableEq

def refLeftJoinPairs (ls : List LRow) (rs : List RRow) : List (LRow × Option RRow) :=
  (ls.filter LRow.pred).flatMap fun l =>
    match l.k with
    | none => [(l, none)]
    | some lk =>
        let matchesR := rs.filter (fun r => r.k == some lk)
        if matchesR.isEmpty then [(l, none)]
        else matchesR.map (fun r => (l, some r))

def sumOptList (vals : List (Option Int)) : Option Int :=
  vals.foldl
    (fun acc v =>
      match acc, v with
      | none, y => y
      | x, none => x
      | some a, some b => some (a + b))
    none

/-- Reference `LEFT OUTER JOIN` aggregation `(COUNT(*), COUNT(L.v), SUM(L.v))`
    over `σ_pred(L) ⟕_{L.k = R.k} R` when `R` columns are unreferenced. -/
def refLeftJoinAgg (ls : List LRow) (rs : List RRow) : Nat × Nat × Option Int :=
  let joined := refLeftJoinPairs ls rs
  let cntStar := joined.length
  let cntVal := (joined.filter (fun lr => lr.1.v.isSome)).length
  let sumVal := sumOptList (joined.map (fun lr => lr.1.v))
  (cntStar, cntVal, sumVal)

def rightKeyMultiplicity (rs : List RRow) (k : String) : Nat :=
  (rs.filter (fun r => r.k == some k)).length

/-- Optimized right-multiplicity weight collapse:
    counts right multiplicities on non-NULL keys only, scans `σ_pred(L)` once,
    and weights each surviving left row by `w = 1` (if `k IS NULL`) or `max 1 mult(k)`. -/
def optMultiplicityAgg (ls : List LRow) (rs : List RRow) : Nat × Nat × Option Int :=
  let st :=
    (ls.filter LRow.pred).foldl
      (fun (acc : Nat × Nat × Bool × Int) (l : LRow) =>
        let (cStar, cVal, seenVal, sVal) := acc
        let w : Nat :=
          match l.k with
          | none => 1
          | some lk => max 1 (rightKeyMultiplicity rs lk)
        match l.v with
        | none => (cStar + w, cVal, seenVal, sVal)
        | some x => (cStar + w, cVal + w, true, sVal + Int.ofNat w * x))
      (0, 0, false, 0)
  (st.1, st.2.1, if st.2.2.1 then some st.2.2.2 else none)

/-- Counterexample showing why matching `NULL = NULL` in right multiplicity breaks
    `LEFT OUTER JOIN` cardinality (`w = 2` instead of `1`). -/
def buggyNullMatchAgg (ls : List LRow) (rs : List RRow) : Nat :=
  (ls.filter LRow.pred).foldl
    (fun acc l =>
      let m := (rs.filter (fun r => r.k == l.k)).length
      acc + max 1 m)
    0

/-- Right-side multiplicity collapse for `LEFT OUTER JOIN` without right-column
    references (STATS-CEB C4): replacing materialization of `R` matches with
    per-non-NULL-key multiplicity weights `max 1 mult(k)` (and `1` for NULL join
    keys) yields identical `(COUNT(*), COUNT(L.v), SUM(L.v))`. -/
def Goal : Prop :=
  ∀ (ls : List LRow) (rs : List RRow),
    optMultiplicityAgg ls rs = refLeftJoinAgg ls rs

example :
    let ls : List LRow :=
      [ { k := none, v := some 10, pred := true }
      , { k := some "sql", v := none, pred := true }
      , { k := some "sql", v := some 7, pred := true }
      , { k := some "other", v := some 5, pred := false }
      , { k := some "miss", v := some 3, pred := true }
      ]
    let rs : List RRow :=
      [ { k := none }, { k := none }
      , { k := some "sql" }, { k := some "sql" }, { k := some "sql" }
      ]
    optMultiplicityAgg ls rs = refLeftJoinAgg ls rs ∧
    optMultiplicityAgg ls rs = (8, 5, some 34) ∧
    buggyNullMatchAgg ls rs ≠ (refLeftJoinAgg ls rs).1 := by
  decide

example :
    optMultiplicityAgg [] [{ k := some "sql" }] = (0, 0, none) ∧
    refLeftJoinAgg [] [{ k := some "sql" }] = (0, 0, none) := by
  decide

end TinylambOpt.ResearchGoals.LeftJoinRightMultiplicityCollapse
