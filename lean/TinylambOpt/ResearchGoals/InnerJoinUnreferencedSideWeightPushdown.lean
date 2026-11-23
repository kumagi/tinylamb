import Std

namespace TinylambOpt.ResearchGoals.InnerJoinUnreferencedSideWeightPushdown

structure LRow where
  g : Int
  k : Option Int
  v : Option Int
  deriving DecidableEq

structure RRow where
  k : Option Int
  pred : Bool
  deriving DecidableEq

structure Agg5 where
  cntStar : Nat
  cntCol : Nat
  sumOpt : Option Int
  minOpt : Option Int
  maxOpt : Option Int
  deriving DecidableEq

def emptyAgg5 : Agg5 :=
  { cntStar := 0, cntCol := 0, sumOpt := none, minOpt := none, maxOpt := none }

def addValWeighted (a : Agg5) (w : Nat) (ov : Option Int) : Agg5 :=
  if w == 0 then a
  else
    match ov with
    | none => { a with cntStar := a.cntStar + w }
    | some x =>
        let newSum :=
          match a.sumOpt with
          | none => some (Int.ofNat w * x)
          | some s => some (s + Int.ofNat w * x)
        let newMin :=
          match a.minOpt with
          | none => some x
          | some m => some (min m x)
        let newMax :=
          match a.maxOpt with
          | none => some x
          | some m => some (max m x)
        { cntStar := a.cntStar + w
          cntCol := a.cntCol + w
          sumOpt := newSum
          minOpt := newMin
          maxOpt := newMax }

/-- Naive flat inner join followed by 5-aggregate computation for group `g`. -/
def refAgg5 (ls : List LRow) (rs : List RRow) (g : Int) : Agg5 :=
  let rsFilt := rs.filter RRow.pred
  let joined :=
    (ls.filter (fun l => l.g == g)).flatMap fun l =>
      rsFilt.filterMap fun r =>
        match l.k, r.k with
        | some lk, some rk => if lk == rk then some l.v else none
        | _, _ => none
  joined.foldl (fun acc ov => addValWeighted acc 1 ov) emptyAgg5

def rightWeight (rsFilt : List RRow) (lk : Int) : Nat :=
  (rsFilt.filter (fun r => r.k == some lk)).length

/-- Weight-pushdown scan for inner equi-join without right-column references:
    summarizes `σ_pred(R)` to non-NULL key multiplicities `w(k)`, skips rows
    with `w == 0` (preserving `MIN`/`MAX` invariance), multiplies additive
    measures by `w`, and ignores multiplicity for idempotent `MIN`/`MAX`. -/
def optAgg5 (ls : List LRow) (rs : List RRow) (g : Int) : Agg5 :=
  let rsFilt := rs.filter RRow.pred
  (ls.filter (fun l => l.g == g)).foldl
    (fun acc l =>
      match l.k with
      | none => acc
      | some lk => addValWeighted acc (rightWeight rsFilt lk) l.v)
    emptyAgg5

/-- Unreferenced right-side multiplicity pushdown for inner equi-joins (BigBench
    B2 / JOB J4, J5 / STATS-CEB C5 / LDBC L5): replacing the right-hand input of
    an inner join whose columns are unreferenced by a per-key count weight table
    matches flat post-join `(COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v))` for
    all groups, provided zero-weight rows are skipped rather than passed to
    `MIN`/`MAX`. -/
def Goal : Prop :=
  ∀ (ls : List LRow) (rs : List RRow) (g : Int),
    optAgg5 ls rs g = refAgg5 ls rs g

example :
    let ls : List LRow :=
      [ { g := 1, k := some 10, v := some 5 }
      , { g := 1, k := some 20, v := some (-100) } -- dead key (pred=false on right)
      , { g := 1, k := some 10, v := none }
      , { g := 1, k := none, v := some 999 }
      , { g := 2, k := some 10, v := some 1 }
      ]
    let rs : List RRow :=
      [ { k := some 10, pred := true }
      , { k := some 10, pred := true }
      , { k := some 10, pred := true }
      , { k := some 20, pred := false }
      , { k := none, pred := true }
      ]
    optAgg5 ls rs 1 = refAgg5 ls rs 1 ∧
    optAgg5 ls rs 1 =
      { cntStar := 6, cntCol := 3, sumOpt := some 15, minOpt := some 5, maxOpt := some 5 } := by
  decide

example :
    optAgg5 [{ g := 1, k := some 5, v := some 42 }] [{ k := some 5, pred := false }] 1 =
      emptyAgg5 ∧
    optAgg5 [] [{ k := some 5, pred := true }] 1 = emptyAgg5 := by
  decide

end TinylambOpt.ResearchGoals.InnerJoinUnreferencedSideWeightPushdown
