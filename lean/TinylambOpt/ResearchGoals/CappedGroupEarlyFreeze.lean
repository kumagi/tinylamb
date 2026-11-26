import Std

/-!
Capped-group early-freeze aggregation (`GROUP BY ... LIMIT K` without `ORDER
BY`/`HAVING`): once `K` distinct keys exist, ignore new unseen keys while still updating
counts for already-inserted keys.

Modeled claims

- `Goal` — Early-frozen hash aggregation for un-ordered `GROUP BY ... LIMIT K`
  (ClickBench Q18): stopping new key allocation at `K` distinct keys while continuing
  updates for existing keys yields `min K (#distinct)` unique groups whose counts
  match exact full-table `COUNT(*)`, whereas stopping the scan prematurely misses
  trailing increments (and adding `ORDER BY`/`HAVING` breaks early freeze).
-/

namespace TinylambOpt.ResearchGoals.CappedGroupEarlyFreeze

def addGroupCount (m : List (Int × Nat)) (k : Int) : List (Int × Nat) :=
  match m with
  | [] => [(k, 1)]
  | (k', c) :: rest =>
      if k' == k then (k', c + 1) :: rest
      else (k', c) :: addGroupCount rest k

def fullGroupCount (rows : List Int) : List (Int × Nat) :=
  rows.foldl addGroupCount []

def addFrozenCount (lim : Nat) (m : List (Int × Nat)) (k : Int) : List (Int × Nat) :=
  match m with
  | [] => if 0 < lim then [(k, 1)] else []
  | (k', c) :: rest =>
      if k' == k then (k', c + 1) :: rest
      else (k', c) :: addFrozenCount (lim - 1) rest k

def stepFrozen (lim : Nat) (m : List (Int × Nat)) (k : Int) : List (Int × Nat) :=
  if m.any (fun p => p.1 == k) || m.length < lim then addGroupCount m k else m

/-- Capped-group early-freeze aggregation (`GROUP BY ... LIMIT K` without
    `ORDER BY`/`HAVING`): once `K` distinct keys exist, ignore new unseen keys
    while still updating counts for already-inserted keys. -/
def frozenGroupCount (lim : Nat) (rows : List Int) : List (Int × Nat) :=
  rows.foldl (stepFrozen lim) []

def badEarlyStopAfterKRows (lim : Nat) (rows : List Int) : List (Int × Nat) :=
  let firstK := fullGroupCount (rows.take lim)
  firstK.take lim

/-- Early-frozen hash aggregation for un-ordered `GROUP BY ... LIMIT K`
    (ClickBench Q18): stopping new key allocation at `K` distinct keys while
    continuing updates for existing keys yields `min K (#distinct)` unique
    groups whose counts match exact full-table `COUNT(*)`, whereas stopping the
    scan prematurely misses trailing increments (and adding `ORDER BY`/`HAVING`
    breaks early freeze). -/
def Goal : Prop :=
  (∀ (lim : Nat) (rows : List Int),
    let out := frozenGroupCount lim rows
    let full := fullGroupCount rows
    out.length = min lim full.length ∧
    (out.map Prod.fst).Nodup ∧
    (∀ p ∈ out, p ∈ full) ∧
    out.length ≤ lim) ∧
  (∃ (lim : Nat) (rows : List Int),
    ∃ p ∈ badEarlyStopAfterKRows lim rows, ¬ (p ∈ fullGroupCount rows))

example :
    let rows : List Int := [1, 2, 3, 1, 2, 1, 4]
    let out := frozenGroupCount 2 rows
    let full := fullGroupCount rows
    out = [(1, 3), (2, 2)] ∧
    out.length = min 2 full.length ∧
    out.all (fun p => full.contains p) = true := by
  decide

example :
    let rows : List Int := [1, 2, 1]
    badEarlyStopAfterKRows 2 rows = [(1, 1), (2, 1)] ∧
    fullGroupCount rows = [(1, 2), (2, 1)] ∧
    frozenGroupCount 0 rows = [] := by
  decide

end TinylambOpt.ResearchGoals.CappedGroupEarlyFreeze
