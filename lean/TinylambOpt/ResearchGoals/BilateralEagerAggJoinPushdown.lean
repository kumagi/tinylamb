import Std

/-!
Naive materialization of the joined pairs `(l, r)` for group `g`.

Reference post-join aggregate `(COUNT(*), SUM(L.v), SUM(R.v))` for group `g`.

Modeled claims

- `Goal` — Bilateral eager aggregation pushdown across an M:N inner equi-join (H2O H5 /
  STATS-CEB C1 / LDBC L2): pre-aggregating both `L` and `R` per join key into
  `(COUNT(*), COUNT(v), SUM(v))` and multiplying orthogonal counts/sums matches full
  pair materialization for every group, including empty, all-NULL, zero-canceling
  sums, and NULL-join-key inputs.
-/

namespace TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown

structure LRow where
  g : Int
  k : Option Int
  v : Option Int
  deriving DecidableEq

structure RRow where
  k : Option Int
  v : Option Int
  deriving DecidableEq

def addUniqueInt (acc : List Int) (x : Int) : List Int :=
  if acc.contains x then acc else x :: acc

def distinctSomeKeys {α : Type} (rows : List α) (getKey : α → Option Int) : List Int :=
  rows.foldl
    (fun acc r =>
      match getKey r with
      | some k => addUniqueInt acc k
      | none => acc)
    []

def sumOptList (vals : List (Option Int)) : Option Int :=
  vals.foldl
    (fun acc v =>
      match acc, v with
      | none, y => y
      | x, none => x
      | some a, some b => some (a + b))
    none

/-- Naive materialization of the joined pairs `(l, r)` for group `g`. -/
def refJoinedPairs (ls : List LRow) (rs : List RRow) (g : Int) : List (LRow × RRow) :=
  (ls.filter (fun l => l.g == g)).flatMap fun l =>
    rs.filterMap fun r =>
      match l.k, r.k with
      | some lk, some rk => if lk == rk then some (l, r) else none
      | _, _ => none

/-- Reference post-join aggregate `(COUNT(*), SUM(L.v), SUM(R.v))` for group `g`. -/
def refGroup (ls : List LRow) (rs : List RRow) (g : Int) :
    Nat × Option Int × Option Int :=
  let pairs := refJoinedPairs ls rs g
  let cnt := pairs.length
  let sumL := sumOptList (pairs.map fun lr => lr.1.v)
  let sumR := sumOptList (pairs.map fun lr => lr.2.v)
  (cnt, sumL, sumR)

structure PreAgg where
  cnt : Nat
  nn : Nat
  sum : Int
  deriving DecidableEq

def preAggRows {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (targetKey : Int) : PreAgg :=
  rows.foldl
    (fun st row =>
      if getKey row == some targetKey then
        match getVal row with
        | none => { st with cnt := st.cnt + 1 }
        | some x =>
            { cnt := st.cnt + 1
              nn := st.nn + 1
              sum := st.sum + x }
      else st)
    { cnt := 0, nn := 0, sum := 0 }

/-- Optimized bilateral pre-aggregation pushdown for group `g`:
    aggregates left (`L`) and right (`R`) independently per join key `k`
    to `(cnt, nonNullCnt, sum)` and combines via Fubini product without
    materializing `M * N` pairs. -/
def optGroup (ls : List LRow) (rs : List RRow) (g : Int) :
    Nat × Option Int × Option Int :=
  let lsG := ls.filter (fun l => l.g == g)
  let ks := distinctSomeKeys lsG LRow.k
  let combined :=
    ks.foldl
      (fun (st : Nat × Nat × Int × Nat × Int) (k : Int) =>
        let (totCnt, totNnL, totSumL, totNnR, totSumR) := st
        let pL := preAggRows lsG LRow.k LRow.v k
        let pR := preAggRows rs RRow.k RRow.v k
        ( totCnt + pL.cnt * pR.cnt
        , totNnL + pL.nn * pR.cnt
        , totSumL + pL.sum * Int.ofNat pR.cnt
        , totNnR + pL.cnt * pR.nn
        , totSumR + Int.ofNat pL.cnt * pR.sum ))
      (0, 0, 0, 0, 0)
  let (totCnt, totNnL, totSumL, totNnR, totSumR) := combined
  ( totCnt
  , if totNnL == 0 then none else some totSumL
  , if totNnR == 0 then none else some totSumR )

/-- Bilateral eager aggregation pushdown across an M:N inner equi-join (H2O H5 /
    STATS-CEB C1 / LDBC L2): pre-aggregating both `L` and `R` per join key into
    `(COUNT(*), COUNT(v), SUM(v))` and multiplying orthogonal counts/sums matches
    full pair materialization for every group, including empty, all-NULL,
    zero-canceling sums, and NULL-join-key inputs. -/
def Goal : Prop :=
  ∀ (ls : List LRow) (rs : List RRow) (g : Int),
    optGroup ls rs g = refGroup ls rs g

private def sampleL : List LRow :=
  [ { g := 1, k := some 10, v := none }
  , { g := 1, k := some 10, v := none }
  , { g := 1, k := some 20, v := some 5 }
  , { g := 1, k := some 20, v := some (-5) }
  , { g := 1, k := none, v := some 99 }
  , { g := 2, k := some 30, v := some 7 }
  ]

private def sampleR : List RRow :=
  [ { k := some 10, v := some 5 }
  , { k := some 10, v := some 2 }
  , { k := some 20, v := none }
  , { k := none, v := some 100 }
  ]

example :
    optGroup sampleL sampleR 1 = refGroup sampleL sampleR 1 ∧
    optGroup sampleL sampleR 1 = (6, some 0, some 14) := by
  decide

example :
    optGroup sampleL sampleR 2 = refGroup sampleL sampleR 2 ∧
    optGroup sampleL sampleR 2 = (0, none, none) ∧
    optGroup ([] : List LRow) sampleR 1 = (0, none, none) := by
  decide

end TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown
