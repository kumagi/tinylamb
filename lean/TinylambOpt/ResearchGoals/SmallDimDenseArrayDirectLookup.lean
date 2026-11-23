import Std

namespace TinylambOpt.ResearchGoals.SmallDimDenseArrayDirectLookup

structure DimRow where
  k : Option Int
  attr : Int
  pred : Bool
  deriving DecidableEq

structure FactRow where
  fk1 : Option Int
  fk2 : Option Int
  val : Option Int
  deriving DecidableEq

def UniqueNonNullDimInRange (rows : List DimRow) (minK : Int) (span : Nat) : Prop :=
  (∀ r : DimRow, r ∈ rows →
    match r.k with
    | none => True
    | some kv => minK ≤ kv ∧ kv < minK + Int.ofNat span) ∧
  (∀ k : Int, (rows.filter (fun r => r.k == some k)).length ≤ 1)

def sumOptList (vals : List (Option Int)) : Option Int :=
  vals.foldl
    (fun acc v =>
      match acc, v with
      | none, y => y
      | x, none => x
      | some a, some b => some (a + b))
    none

/-- Reference 2-step hash join `F ⋈_{fk1=k1} (σ_{p1} D1) ⋈_{fk2=k2} (σ_{p2} D2)`
    aggregated by `(d1.attr, d2.attr)`. -/
def refTwoHashJoins (d1 d2 : List DimRow) (facts : List FactRow) (a1 a2 : Int) :
    Nat × Option Int :=
  let d1F := d1.filter DimRow.pred
  let d2F := d2.filter DimRow.pred
  let joined :=
    facts.flatMap fun f =>
      d1F.flatMap fun r1 =>
        match f.fk1, r1.k with
        | some fk1, some k1 =>
            if fk1 == k1 then
              d2F.filterMap fun r2 =>
                match f.fk2, r2.k with
                | some fk2, some k2 =>
                    if fk2 == k2 && r1.attr == a1 && r2.attr == a2 then some f.val else none
                | _, _ => none
            else []
        | _, _ => []
  (joined.length, sumOptList joined)

def buildDenseDimTable (dim : List DimRow) (minK : Int) (span : Nat) :
    List (Option Int) :=
  (List.range span).map fun idx =>
    let targetK := minK + Int.ofNat idx
    match dim.find? (fun r => r.pred && r.k == some targetK) with
    | some r => some r.attr
    | none => none

def lookupDense (tbl : List (Option Int)) (minK : Int) (span : Nat)
    (fk : Option Int) : Option Int :=
  match fk with
  | none => none
  | some k =>
      if k < minK || minK + Int.ofNat span ≤ k then none
      else tbl.getD (k - minK).toNat none

/-- Single-pass star-schema dense-array direct lookup without hash probes or
    intermediate materialized join rows. -/
def optStarDirect (d1 d2 : List DimRow) (min1 : Int) (span1 : Nat)
    (min2 : Int) (span2 : Nat) (facts : List FactRow) (a1 a2 : Int) :
    Nat × Option Int :=
  let arr1 := buildDenseDimTable d1 min1 span1
  let arr2 := buildDenseDimTable d2 min2 span2
  let matchedVals :=
    facts.filterMap fun f =>
      match lookupDense arr1 min1 span1 f.fk1, lookupDense arr2 min2 span2 f.fk2 with
      | some attr1, some attr2 =>
          if attr1 == a1 && attr2 == a2 then some f.val else none
      | _, _ => none
  (matchedVals.length, sumOptList matchedVals)

private def dupDim1 : List DimRow :=
  [ { k := some 0, attr := 10, pred := true }
  , { k := some 0, attr := 10, pred := true } -- duplicate key!
  ]

private def singleDim2 : List DimRow :=
  [ { k := some 0, attr := 20, pred := true } ]

private def dupFact : List FactRow :=
  [ { fk1 := some 0, fk2 := some 0, val := some 5 } ]

/-- Small-dimension dense direct-index lookup for star joins (BigBench B3 / LDBC
    L5 / JOB J4 / STATS-CEB C2, C5): when both dimension tables have unique
    non-NULL keys bounded within `[min, min + span)`, direct O(1) array indexing
    matches two hash joins for all facts (including out-of-range, negative, and
    NULL FKs), whereas duplicate dimension keys break direct-slot collapse. -/
def Goal : Prop :=
  (∀ (d1 d2 : List DimRow) (min1 : Int) (span1 : Nat)
     (min2 : Int) (span2 : Nat) (facts : List FactRow) (a1 a2 : Int),
    UniqueNonNullDimInRange d1 min1 span1 →
    UniqueNonNullDimInRange d2 min2 span2 →
    optStarDirect d1 d2 min1 span1 min2 span2 facts a1 a2 =
      refTwoHashJoins d1 d2 facts a1 a2) ∧
  (optStarDirect dupDim1 singleDim2 0 2 0 2 dupFact 10 20 ≠
    refTwoHashJoins dupDim1 singleDim2 dupFact 10 20)

example :
    let d1 : List DimRow :=
      [ { k := some (-2), attr := 1, pred := true }
      , { k := some 0, attr := 1, pred := false }
      , { k := none, attr := 1, pred := true }
      ]
    let d2 : List DimRow :=
      [ { k := some 5, attr := 9, pred := true }
      , { k := some 6, attr := 9, pred := true }
      ]
    let facts : List FactRow :=
      [ { fk1 := some (-2), fk2 := some 5, val := some 10 }
      , { fk1 := some (-2), fk2 := some 6, val := none }
      , { fk1 := some (-99), fk2 := some 5, val := some 99 } -- out of range left
      , { fk1 := some 0, fk2 := some 5, val := some 77 }     -- filtered out on d1
      , { fk1 := none, fk2 := some 5, val := some 55 }       -- NULL FK
      , { fk1 := some (-2), fk2 := some 100, val := some 33 } -- out of range right
      ]
    optStarDirect d1 d2 (-2) 4 5 3 facts 1 9 = refTwoHashJoins d1 d2 facts 1 9 ∧
    optStarDirect d1 d2 (-2) 4 5 3 facts 1 9 = (2, some 10) := by
  decide

example :
    optStarDirect dupDim1 singleDim2 0 2 0 2 dupFact 10 20 = (1, some 5) ∧
    refTwoHashJoins dupDim1 singleDim2 dupFact 10 20 = (2, some 10) := by
  decide

end TinylambOpt.ResearchGoals.SmallDimDenseArrayDirectLookup
