import TinylambOpt.Research.TieWatermark
import TinylambOpt.Research.IncrementalSortPage

/-!
Rank after OFFSET. All rows strictly ahead count, including duplicates; peers at the
boundary share their rank. Input is in SQL order.

Modeled claims

- `Goal` — The stable tie order used here is stream concatenation order. For the
  incremental sorter, occurrence IDs encode that order.
-/

namespace TinylambOpt.ResearchGoals.SqlFetchWithTies

open Research

/-- Rank after OFFSET. All rows strictly ahead count, including duplicates;
    peers at the boundary share their rank. Input is in SQL order. -/
def fetch {α : Type} (lt : α → α → Bool) (offset k : Nat) (ordered : List α) : List α :=
  let after := ordered.drop offset
  after.filter fun row => (after.filter fun other => lt other row).length < k

def component (d : TieWatermark.Dir) (n : TieWatermark.Nulls) : Option Int → Option Int → Ordering
  | none, none => .eq
  | none, some _ => if n == .First then .lt else .gt
  | some _, none => if n == .First then .gt else .lt
  | some x, some y => if d == .ASC then compare x y else compare y x

def lex : TieWatermark.Spec → TieWatermark.Key → TieWatermark.Key → Ordering
  | (d,n) :: ds, a :: as, b :: bs =>
    match component d n a b with
    | .eq => lex ds as bs
    | c => c
  | _, _, _ => .eq

def watermarkSQL (spec : TieWatermark.Spec) (offset k : Nat) (rows : List TieWatermark.Row) :=
  let cmp := fun (x y : TieWatermark.Row) => lex spec x.key y.key
  fetch (fun x y => cmp x y == .lt) offset k
    (rows.mergeSort fun x y => cmp x y != .gt)

def pageSQL (offset k : Nat) (rows : List IncrementalSortPage.Row) :=
  let cmp := fun (a b : IncrementalSortPage.Row) =>
    if a.pkey = b.pkey then compare a.skey b.skey else compare a.pkey b.pkey
  fetch (fun x y => cmp x y == .lt) offset k
    (rows.mergeSort fun x y => cmp x y != .gt)

/-- The stable tie order used here is stream concatenation order. For the
    incremental sorter, occurrence IDs encode that order. -/
def Goal : Prop :=
  (∀ (spec : TieWatermark.Spec) offset k (streams : List (List TieWatermark.Row)),
    (∀ r ∈ streams.flatten, r.key.length = spec.length) →
    (TieWatermark.wmRead spec offset k (TieWatermark.mergeStreams spec streams)).1 =
      watermarkSQL spec offset k streams.flatten) ∧
  (∀ offset k (files : List (List IncrementalSortPage.Row)), IncrementalSortPage.PrefixSorted files.flatten →
    files.flatten.Pairwise (fun a b => a.id < b.id) →
    (IncrementalSortPage.incPage offset k (IncrementalSortPage.runSort files)).1 = pageSQL offset k files.flatten)

private def rows : List TieWatermark.Row :=
  [⟨[none], 9, 0⟩, ⟨[some 2], 2, 1⟩, ⟨[some 1], 1, 2⟩, ⟨[some 2], 3, 3⟩]
private def spec : TieWatermark.Spec := [(.DESC, .Last)]

example : (watermarkSQL spec 0 1 rows).map TieWatermark.Row.payload = [2, 3] := by cbv
example : (watermarkSQL spec 1 1 rows).map TieWatermark.Row.payload = [3] := by cbv
example : (watermarkSQL spec 0 4 rows).map TieWatermark.Row.payload = [2, 3, 1, 9] := by cbv
example : pageSQL 0 0 [⟨0, 0, 7, 0⟩] = [] := by cbv

theorem zero_sql (offset : Nat) (spec : TieWatermark.Spec) (streams : List (List TieWatermark.Row))
    (files : List (List IncrementalSortPage.Row)) :
    (TieWatermark.wmRead spec offset 0 (TieWatermark.mergeStreams spec streams)).1 =
      watermarkSQL spec offset 0 streams.flatten ∧
    (IncrementalSortPage.incPage offset 0 (IncrementalSortPage.runSort files)).1 = pageSQL offset 0 files.flatten := by
  simp [TieWatermark.wmRead, IncrementalSortPage.incPage, watermarkSQL, pageSQL, fetch]

end TinylambOpt.ResearchGoals.SqlFetchWithTies
