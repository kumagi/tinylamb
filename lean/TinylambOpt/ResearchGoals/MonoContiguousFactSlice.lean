import Std

/-!
If NULLs (`none`) appear in the middle of an otherwise sorted array and sort as
`-infinity` below `lo`, binary search can overshoot and miss valid matches to the right.

Modeled claims

- `Goal` — Monotone contiguous compressed fact binary-search slice
  (`executor/detail/fused_grouped_join.cpp` `mono_contiguous_rf`, TPC-DS Q65): over a
  non-null key-sorted fact column, `bsLower lo .. bsUpper hi` selects a contiguous
  range covering every row satisfying any predicate whose support lies within `[lo,
  hi]` (even when `lo > hi`).
-/

namespace TinylambOpt.ResearchGoals.MonoContiguousFactSlice

def SortedByKey (rows : List (Int × Int)) : Prop :=
  (rows.map Prod.fst).Pairwise (· ≤ ·)

def bsLower (rows : List (Int × Int)) (target : Int) : Nat :=
  let rec go (l r fuel : Nat) : Nat :=
    match fuel with
    | 0 => l
    | f + 1 =>
        if r ≤ l then l
        else
          let mid := (l + r) / 2
          match rows[mid]? with
          | some p => if p.1 < target then go (mid + 1) r f else go l mid f
          | none => l
  go 0 rows.length (rows.length + 1)

def bsUpper (rows : List (Int × Int)) (target : Int) : Nat :=
  let rec go (l r fuel : Nat) : Nat :=
    match fuel with
    | 0 => l
    | f + 1 =>
        if r ≤ l then l
        else
          let mid := (l + r) / 2
          match rows[mid]? with
          | some p => if p.1 ≤ target then go (mid + 1) r f else go l mid f
          | none => l
  go 0 rows.length (rows.length + 1)

def refFilter (rows : List (Int × Int)) (pred : Int → Bool) : List (Int × Int) :=
  rows.filter (fun r => pred r.1)

def sliceFilter (rows : List (Int × Int)) (lo hi : Int) (pred : Int → Bool) : List (Int × Int) :=
  let lb := bsLower rows lo
  let ub := bsUpper rows hi
  ((rows.drop lb).take (ub - lb)).filter (fun r => pred r.1)

/-- Monotone contiguous compressed fact binary-search slice
    (`executor/detail/fused_grouped_join.cpp` `mono_contiguous_rf`, TPC-DS Q65):
    over a non-null key-sorted fact column, `bsLower lo .. bsUpper hi` selects a
    contiguous range covering every row satisfying any predicate whose support lies
    within `[lo, hi]` (even when `lo > hi`). -/
def Goal : Prop :=
  ∀ (rows : List (Int × Int)) (lo hi : Int) (pred : Int → Bool),
    SortedByKey rows →
    (∀ k : Int, pred k = true → lo ≤ k ∧ k ≤ hi) →
    let lb := bsLower rows lo
    let ub := bsUpper rows hi
    sliceFilter rows lo hi pred = refFilter rows pred ∧
    ∀ (i : Nat) (r : Int × Int),
      rows[i]? = some r → (i < lb ∨ ub ≤ i) → pred r.1 = false

/-- If NULLs (`none`) appear in the middle of an otherwise sorted array and
    sort as `-infinity` below `lo`, binary search can overshoot and miss valid
    matches to the right. -/
def nullableBsLower (rows : List (Option Int × Int)) (target : Int) : Nat :=
  let rec go (l r fuel : Nat) : Nat :=
    match fuel with
    | 0 => l
    | f + 1 =>
        if r ≤ l then l
        else
          let mid := (l + r) / 2
          match rows[mid]? with
          | some (some k, _) => if k < target then go (mid + 1) r f else go l mid f
          | some (none, _) => go (mid + 1) r f
          | none => l
  go 0 rows.length (rows.length + 1)

private def sortedEx : List (Int × Int) :=
  [(1, 10), (3, 20), (3, 30), (5, 40), (8, 50), (10, 60)]

example :
    sliceFilter sortedEx 3 5 (fun k => k == 3 || k == 5) =
      refFilter sortedEx (fun k => k == 3 || k == 5) ∧
    sliceFilter sortedEx 7 2 (fun _ => false) = [] := by
  decide

example :
    let withMidNull : List (Option Int × Int) := [(some 5, 1), (none, 2), (some 5, 3)]
    nullableBsLower withMidNull 5 = 2 := by
  decide

end TinylambOpt.ResearchGoals.MonoContiguousFactSlice
