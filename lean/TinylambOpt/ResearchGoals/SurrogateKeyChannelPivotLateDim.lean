import Std

/-!
Reference: join dimension before grouping/pivoting in wide `(bk, wide)` space.

Optimized: aggregate `fSmall` directly by integer `sk`, semijoin-filter `fLarge` to
valid `sk`s with `s1 > 0` before aggregating `fLarge`, pivot in integer `sk` space, and
materialize wide dimension attributes at the end.

Modeled claims

- `Goal` — Integer surrogate-key channel pivot + semijoin narrowing + late wide-column
  dimension lookup (TPC-DS Q04/Q11/Q74/Q79): when `sk` is unique in `D` and `sk ↦ bk`
  is injective, pivoting on integer `sk`, narrowing large channels by valid `y1 > 0`
  `sk`s, and attaching wide attributes only for winners equals wide-string pre-join
  grouping, whereas non-injective `bk` breaks it.
-/

namespace TinylambOpt.ResearchGoals.SurrogateKeyChannelPivotLateDim

structure DimRow where
  sk : Nat
  bk : String
  wide : String
  deriving DecidableEq

structure FactRow where
  sk : Nat
  year : Nat
  amt : Int
  deriving DecidableEq

structure PivotOut where
  bk : String
  wide : String
  s1 : Int
  s2 : Int
  l1 : Int
  l2 : Int
  deriving DecidableEq

def UniqueSk (d : List DimRow) : Prop := (d.map DimRow.sk).Nodup

def InjectiveBk (d : List DimRow) : Prop :=
  ∀ a ∈ d, ∀ b ∈ d, a.bk = b.bk → a.sk = b.sk

def insertSortedOut (x : PivotOut) : List PivotOut → List PivotOut
  | [] => [x]
  | y :: ys => if decide (x.bk ≤ y.bk) then x :: y :: ys else y :: insertSortedOut x ys

def sortOut : List PivotOut → List PivotOut
  | [] => []
  | x :: xs => insertSortedOut x (sortOut xs)

def sumForBkYear (d : List DimRow) (f : List FactRow) (bk : String) (yr : Nat) :
    Bool × Int :=
  f.foldl
    (fun (seen, acc) fr =>
      if fr.year != yr then (seen, acc)
      else
        d.foldl
          (fun (sn, sm) dr =>
            if dr.sk == fr.sk && dr.bk == bk then (true, sm + fr.amt) else (sn, sm))
          (seen, acc))
    (false, 0)

def lookupWideForBk (d : List DimRow) (bk : String) : String :=
  match d.find? (fun dr => dr.bk == bk) with
  | none => ""
  | some dr => dr.wide

def uniqBks (d : List DimRow) : List String :=
  (d.map DimRow.bk).foldl
    (fun acc bk => if acc.contains bk then acc else acc ++ [bk]) []

/-- Reference: join dimension before grouping/pivoting in wide `(bk, wide)` space. -/
def refPivot (d : List DimRow) (fSmall fLarge : List FactRow) : List PivotOut :=
  let out :=
    (uniqBks d).filterMap fun bk =>
      let (hs1, s1) := sumForBkYear d fSmall bk 1
      let (hs2, s2) := sumForBkYear d fSmall bk 2
      let (hl1, l1) := sumForBkYear d fLarge bk 1
      let (hl2, l2) := sumForBkYear d fLarge bk 2
      if hs1 && hs2 && hl1 && hl2 && 0 < s1 && 0 < l1 && l2 * s1 > s2 * l1 then
        some ⟨bk, lookupWideForBk d bk, s1, s2, l1, l2⟩
      else none
  sortOut out

def sumForSkYear (f : List FactRow) (sk : Nat) (yr : Nat) : Bool × Int :=
  f.foldl
    (fun (seen, acc) fr =>
      if fr.sk == sk && fr.year == yr then (true, acc + fr.amt) else (seen, acc))
    (false, 0)

/-- Optimized: aggregate `fSmall` directly by integer `sk`, semijoin-filter
    `fLarge` to valid `sk`s with `s1 > 0` before aggregating `fLarge`, pivot in
    integer `sk` space, and materialize wide dimension attributes at the end. -/
def optSkPivotLateDim (d : List DimRow) (fSmall fLarge : List FactRow) :
    List PivotOut :=
  let validSks :=
    d.filterMap fun dr =>
      let (hs1, s1) := sumForSkYear fSmall dr.sk 1
      let (hs2, s2) := sumForSkYear fSmall dr.sk 2
      if hs1 && hs2 && 0 < s1 then some (dr, s1, s2) else none
  let activeSks := validSks.map (fun (dr, _, _) => dr.sk)
  let fLargeNarrow := fLarge.filter (fun fr => activeSks.contains fr.sk)
  let out :=
    validSks.filterMap fun (dr, s1, s2) =>
      let (hl1, l1) := sumForSkYear fLargeNarrow dr.sk 1
      let (hl2, l2) := sumForSkYear fLargeNarrow dr.sk 2
      if hl1 && hl2 && 0 < l1 && l2 * s1 > s2 * l1 then
        some ⟨dr.bk, dr.wide, s1, s2, l1, l2⟩
      else none
  sortOut out

/-- Integer surrogate-key channel pivot + semijoin narrowing + late wide-column
    dimension lookup (TPC-DS Q04/Q11/Q74/Q79): when `sk` is unique in `D` and
    `sk ↦ bk` is injective, pivoting on integer `sk`, narrowing large channels by
    valid `y1 > 0` `sk`s, and attaching wide attributes only for winners equals
    wide-string pre-join grouping, whereas non-injective `bk` breaks it. -/
def Goal : Prop :=
  (∀ (d : List DimRow) (fSmall fLarge : List FactRow),
    UniqueSk d → InjectiveBk d →
      optSkPivotLateDim d fSmall fLarge = refPivot d fSmall fLarge) ∧
  (∃ (d : List DimRow) (fSmall fLarge : List FactRow),
    UniqueSk d ∧ optSkPivotLateDim d fSmall fLarge ≠ refPivot d fSmall fLarge)

private def injDim : List DimRow :=
  [{ sk := 1, bk := "C1", wide := "Alice" }, { sk := 2, bk := "C2", wide := "Bob" }]
private def dupBkDim : List DimRow :=
  [{ sk := 1, bk := "C1", wide := "Alice" }, { sk := 2, bk := "C1", wide := "Alice" }]

example :
    let fS : List FactRow := [{ sk := 1, year := 1, amt := 10 }, { sk := 1, year := 2, amt := 15 }]
    let fL : List FactRow := [{ sk := 1, year := 1, amt := 20 }, { sk := 1, year := 2, amt := 40 },
                              { sk := 2, year := 1, amt := 5 }, { sk := 2, year := 2, amt := 99 }]
    optSkPivotLateDim injDim fS fL = refPivot injDim fS fL ∧
    optSkPivotLateDim injDim fS fL = [⟨"C1", "Alice", 10, 15, 20, 40⟩] := by
  decide

example :
    let fS : List FactRow := [{ sk := 1, year := 1, amt := 10 }, { sk := 2, year := 2, amt := 10 }]
    let fL : List FactRow := [{ sk := 1, year := 1, amt := 10 }, { sk := 2, year := 2, amt := 30 }]
    optSkPivotLateDim dupBkDim fS fL ≠ refPivot dupBkDim fS fL := by
  decide

end TinylambOpt.ResearchGoals.SurrogateKeyChannelPivotLateDim
