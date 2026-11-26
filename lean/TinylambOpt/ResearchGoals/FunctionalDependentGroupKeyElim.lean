import Std

/-!
Reference strict-overflow `GROUP BY k, k-1, k-2, k-3 ORDER BY COUNT(*) DESC LIMIT K`:
fails (`none`) if any input row overflows `int64` on `k - 1`, `k - 2`, or `k - 3`.

Optimized functional-key reduction: group ONLY by `k`, take Top-`K`, then compute `(k-1,
k-2, k-3)` only on the `≤ K` output rows (guarded by explicit per-row overflow check
`int64Min + 3 ≤ k ≤ int64Max` when domain isn't bounded).

Modeled claims

- `Goal` — Functional-dependent group-key elimination (`GROUP BY k, k-1, k-2, k-3` on
  ClickBench Q36): when all inputs satisfy `k ∈ [MIN_I64 + 3, MAX_I64]`, grouping
  only by `k` and materializing `(k-1, k-2, k-3)` after `Top-K` yields exact equality
  with 4-key grouping; deferring `k-1` evaluation past `LIMIT K` without checking all
  inputs silently hides overflow errors on dropped rows.
-/

namespace TinylambOpt.ResearchGoals.FunctionalDependentGroupKeyElim

def int64Min : Int := -9223372036854775808
def int64Max : Int := 9223372036854775807

def checkedSub (k d : Int) : Option Int :=
  let v := k - d
  if decide (int64Min ≤ v ∧ v ≤ int64Max) then some v else none

structure FullKey where
  k : Int
  km1 : Int
  km2 : Int
  km3 : Int
  deriving DecidableEq

def fullKeyOf (k : Int) : Option FullKey :=
  match checkedSub k 1, checkedSub k 2, checkedSub k 3 with
  | some a, some b, some c => some ⟨k, a, b, c⟩
  | _, _, _ => none

def addFullCount (m : List (FullKey × Nat)) (fk : FullKey) : List (FullKey × Nat) :=
  match m with
  | [] => [(fk, 1)]
  | (fk', c) :: rest =>
      if fk' == fk then (fk', c + 1) :: rest
      else (fk', c) :: addFullCount rest fk

def betterFull (a b : FullKey × Nat) : Bool :=
  if a.2 != b.2 then decide (b.2 ≤ a.2) else decide (a.1.k ≤ b.1.k)

def insertSortedF (x : FullKey × Nat) : List (FullKey × Nat) → List (FullKey × Nat)
  | [] => [x]
  | y :: ys => if betterFull x y then x :: y :: ys else y :: insertSortedF x ys

def sortFull : List (FullKey × Nat) → List (FullKey × Nat)
  | [] => []
  | x :: xs => insertSortedF x (sortFull xs)

/-- Reference strict-overflow `GROUP BY k, k-1, k-2, k-3 ORDER BY COUNT(*) DESC LIMIT K`:
    fails (`none`) if any input row overflows `int64` on `k - 1`, `k - 2`, or `k - 3`. -/
def refGroupedTopK (lim : Nat) (rows : List Int) : Option (List (FullKey × Nat)) :=
  let keysOpt : Option (List FullKey) :=
    rows.foldr
      (fun k acc =>
        match fullKeyOf k, acc with
        | some fk, some ks => some (fk :: ks)
        | _, _ => none)
      (some [])
  keysOpt.map fun fks =>
    let grouped := fks.foldl addFullCount []
    (sortFull grouped).take lim

def addKeyCount (m : List (Int × Nat)) (k : Int) : List (Int × Nat) :=
  match m with
  | [] => [(k, 1)]
  | (k', c) :: rest =>
      if k' == k then (k', c + 1) :: rest
      else (k', c) :: addKeyCount rest k

def betterKey (a b : Int × Nat) : Bool :=
  if a.2 != b.2 then decide (b.2 ≤ a.2) else decide (a.1 ≤ b.1)

def insertSortedK (x : Int × Nat) : List (Int × Nat) → List (Int × Nat)
  | [] => [x]
  | y :: ys => if betterKey x y then x :: y :: ys else y :: insertSortedK x ys

def sortKey : List (Int × Nat) → List (Int × Nat)
  | [] => []
  | x :: xs => insertSortedK x (sortKey xs)

/-- Optimized functional-key reduction: group ONLY by `k`, take Top-`K`, then
    compute `(k-1, k-2, k-3)` only on the `≤ K` output rows (guarded by explicit
    per-row overflow check `int64Min + 3 ≤ k ≤ int64Max` when domain isn't bounded). -/
def optKeyOnlyTopK (lim : Nat) (rows : List Int) : Option (List (FullKey × Nat)) :=
  let allSafe := rows.all (fun k => decide (int64Min + 3 ≤ k ∧ k ≤ int64Max))
  if !allSafe then none
  else
    let topK := (sortKey (rows.foldl addKeyCount [])).take lim
    some (topK.map fun (k, c) => (⟨k, k - 1, k - 2, k - 3⟩, c))

def badUncheckedDeferTopK (lim : Nat) (rows : List Int) : Option (List (FullKey × Nat)) :=
  let topK := (sortKey (rows.foldl addKeyCount [])).take lim
  topK.foldr
    (fun (k, c) acc =>
      match fullKeyOf k, acc with
      | some fk, some rest => some ((fk, c) :: rest)
      | _, _ => none)
    (some [])

/-- Functional-dependent group-key elimination (`GROUP BY k, k-1, k-2, k-3`
    on ClickBench Q36):
    when all inputs satisfy `k ∈ [MIN_I64 + 3, MAX_I64]`, grouping only by `k`
    and materializing `(k-1, k-2, k-3)` after `Top-K` yields exact equality with
    4-key grouping; deferring `k-1` evaluation past `LIMIT K` without checking
    all inputs silently hides overflow errors on dropped rows. -/
def Goal : Prop :=
  (∀ (lim : Nat) (rows : List Int),
    optKeyOnlyTopK lim rows = refGroupedTopK lim rows) ∧
  (∃ (lim : Nat) (rows : List Int),
    badUncheckedDeferTopK lim rows ≠ refGroupedTopK lim rows)

example :
    let rows : List Int := [10, 20, 10, 30, 10, 20]
    optKeyOnlyTopK 2 rows = refGroupedTopK 2 rows ∧
    optKeyOnlyTopK 2 rows = some [(⟨10, 9, 8, 7⟩, 3), (⟨20, 19, 18, 17⟩, 2)] := by
  decide

example :
    let rows : List Int := [10, 10, int64Min]
    optKeyOnlyTopK 1 rows = none ∧
    refGroupedTopK 1 rows = none ∧
    badUncheckedDeferTopK 1 rows = some [(⟨10, 9, 8, 7⟩, 2)] := by
  decide

end TinylambOpt.ResearchGoals.FunctionalDependentGroupKeyElim
