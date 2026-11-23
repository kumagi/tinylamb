import Std

namespace TinylambOpt.ResearchGoals.StarHubMultiwayWeightedProduct

structure HRow where
  k : Option Int
  g : Int
  pred : Bool
  deriving DecidableEq

structure B1Row where
  k : Option Int
  v : Option Int
  pred : Bool
  deriving DecidableEq

structure B2Row where
  k : Option Int
  pred : Bool
  deriving DecidableEq

def sumOptList (vals : List (Option Int)) : Option Int :=
  vals.foldl
    (fun acc v =>
      match acc, v with
      | none, y => y
      | x, none => x
      | some a, some b => some (a + b))
    none

/-- Naive 3-way FK-FK star-hub join `(H ⋈_{k} B1) ⋈_{k} B2` followed by
    `GROUP BY H.g` computing `(COUNT(*), COUNT(B1.v), SUM(B1.v))`. -/
def refStarHub (hs : List HRow) (b1s : List B1Row) (b2s : List B2Row) (g : Int) :
    Nat × Nat × Option Int :=
  let hsF := hs.filter (fun h => h.pred && h.g == g)
  let b1F := b1s.filter B1Row.pred
  let b2F := b2s.filter B2Row.pred
  let triples :=
    hsF.flatMap fun h =>
      b1F.flatMap fun b1 =>
        b2F.filterMap fun b2 =>
          match h.k, b1.k, b2.k with
          | some kh, some k1, some k2 =>
              if kh == k1 && kh == k2 then some b1.v else none
          | _, _, _ => none
  ( triples.length
  , (triples.filter Option.isSome).length
  , sumOptList triples )

structure B1Summary where
  cnt : Nat
  nn : Nat
  sum : Int
  deriving DecidableEq

def summarizeB1 (b1F : List B1Row) (k : Int) : B1Summary :=
  b1F.foldl
    (fun st r =>
      if r.k == some k then
        match r.v with
        | none => { st with cnt := st.cnt + 1 }
        | some x => { cnt := st.cnt + 1, nn := st.nn + 1, sum := st.sum + x }
      else st)
    { cnt := 0, nn := 0, sum := 0 }

def countB2 (b2F : List B2Row) (k : Int) : Nat :=
  (b2F.filter (fun r => r.k == some k)).length

/-- Star-hub 3-way factorization without FK-FK blowup: summarizes `B1` per key
    to `(c1, nn1, s1)` and `B2` per key to `c2`, then scans `H` once, summing
    `c1 * c2`, `nn1 * c2`, and `s1 * c2` per matching hub row. -/
def optStarHub (hs : List HRow) (b1s : List B1Row) (b2s : List B2Row) (g : Int) :
    Nat × Nat × Option Int :=
  let hsF := hs.filter (fun h => h.pred && h.g == g)
  let b1F := b1s.filter B1Row.pred
  let b2F := b2s.filter B2Row.pred
  let st :=
    hsF.foldl
      (fun (acc : Nat × Nat × Int) (h : HRow) =>
        match h.k with
        | none => acc
        | some kh =>
            let s1 := summarizeB1 b1F kh
            let c2 := countB2 b2F kh
            ( acc.1 + s1.cnt * c2
            , acc.2.1 + s1.nn * c2
            , acc.2.2 + s1.sum * Int.ofNat c2 ))
      (0, 0, 0)
  (st.1, st.2.1, if st.2.1 == 0 then none else some st.2.2)

/-- Star-hub 3-way FK-FK weighted-product factorization (STATS-CEB C1, C5 /
    JOB J5 / LDBC L2 / BigBench B3): independent pre-aggregation of value branch
    `B1` and multiplicity branch `B2` keyed by hub FK `k`, combined in a single
    scan over `H`, avoids intermediate FK-FK tuple blowup while preserving exact
    `(COUNT(*), COUNT(B1.v), SUM(B1.v))`. -/
def Goal : Prop :=
  ∀ (hs : List HRow) (b1s : List B1Row) (b2s : List B2Row) (g : Int),
    optStarHub hs b1s b2s g = refStarHub hs b1s b2s g

example :
    let hs : List HRow :=
      [ { k := some 1, g := 10, pred := true }
      , { k := some 1, g := 10, pred := true } -- duplicate key on hub
      , { k := some 2, g := 10, pred := true } -- all-NULL values in B1
      , { k := none, g := 10, pred := true }
      , { k := some 1, g := 20, pred := true }
      ]
    let b1s : List B1Row :=
      [ { k := some 1, v := some 5, pred := true }
      , { k := some 1, v := some (-5), pred := true }
      , { k := some 1, v := none, pred := true }
      , { k := some 2, v := none, pred := true }
      , { k := some 1, v := some 100, pred := false }
      ]
    let b2s : List B2Row :=
      [ { k := some 1, pred := true }
      , { k := some 1, pred := true }
      , { k := some 2, pred := true }
      , { k := none, pred := true }
      ]
    optStarHub hs b1s b2s 10 = refStarHub hs b1s b2s 10 ∧
    optStarHub hs b1s b2s 10 = (13, 8, some 0) ∧
    optStarHub hs [{ k := some 2, v := none, pred := true }] b2s 10 =
      refStarHub hs [{ k := some 2, v := none, pred := true }] b2s 10 := by
  decide

example :
    let hs : List HRow := [{ k := some 1, g := 10, pred := true }]
    let b1s : List B1Row := [{ k := some 1, v := some 5, pred := true }]
    let b2s : List B2Row := []
    optStarHub hs b1s b2s 10 = (0, 0, none) ∧
    refStarHub hs b1s b2s 10 = (0, 0, none) := by
  decide

end TinylambOpt.ResearchGoals.StarHubMultiwayWeightedProduct
