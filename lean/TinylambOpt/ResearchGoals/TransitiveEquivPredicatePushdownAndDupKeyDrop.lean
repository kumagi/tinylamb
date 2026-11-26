import Std

/-!
Reference 3-way inner join checking BOTH `b.k = c.k` AND redundant `a.k = c.k`.

Optimized 3-way inner join: pushes transitive semijoin filter from `A` into `C` (`kc ∈ {
a.k | a ∈ A }`) and drops duplicate key `a.k = c.k` on the second hash join (`keys=2 ->
keys=1`).

Modeled claims

- `Goal` — Transitive equivalence key closure, semijoin pushdown, and duplicate
  hash-key dropping (JOB J5 / STATS-CEB C1, C5 / LDBC L1, L2 / JOB J4): for inner
  joins `A ⋈_{a.k = b.k} B ⋈_{b.k = c.k ∧ a.k = c.k} C`, pushing `A`'s key domain
  into `C` and eliding the redundant `a.k = c.k` comparison (`keys=2 -> keys=1`) is
  unconditionally sound, whereas dropping the `B`-side check across a `LEFT OUTER
  JOIN` (`A ⟕ B`) changes results.
-/

namespace TinylambOpt.ResearchGoals.TransitiveEquivPredicatePushdownAndDupKeyDrop

structure RowA where
  id : Nat
  k : Option Int
  deriving DecidableEq

structure RowB where
  id : Nat
  k : Option Int
  deriving DecidableEq

structure RowC where
  id : Nat
  k : Option Int
  deriving DecidableEq

def innerAB (as : List RowA) (bs : List RowB) : List (RowA × RowB) :=
  as.flatMap fun a =>
    bs.filterMap fun b =>
      match a.k, b.k with
      | some ka, some kb => if ka == kb then some (a, b) else none
      | _, _ => none

def leftAB (as : List RowA) (bs : List RowB) : List (RowA × Option RowB) :=
  as.flatMap fun a =>
    match a.k with
    | none => [(a, none)]
    | some ka =>
        let hits := bs.filter (fun b => b.k == some ka)
        if hits.isEmpty then [(a, none)]
        else hits.map (fun b => (a, some b))

/-- Reference 3-way inner join checking BOTH `b.k = c.k` AND redundant `a.k = c.k`. -/
def refInner3 (as : List RowA) (bs : List RowB) (cs : List RowC) :
    List (Nat × Nat × Nat) :=
  (innerAB as bs).flatMap fun (a, b) =>
    cs.filterMap fun c =>
      match a.k, b.k, c.k with
      | some ka, some kb, some kc =>
          if kb == kc && ka == kc then some (a.id, b.id, c.id) else none
      | _, _, _ => none

/-- Optimized 3-way inner join: pushes transitive semijoin filter from `A`
    into `C` (`kc ∈ { a.k | a ∈ A }`) and drops duplicate key `a.k = c.k` on the
    second hash join (`keys=2 -> keys=1`). -/
def optInner3 (as : List RowA) (bs : List RowB) (cs : List RowC) :
    List (Nat × Nat × Nat) :=
  let aKeys := as.filterMap RowA.k
  let csPruned :=
    cs.filter fun c =>
      match c.k with
      | some kc => aKeys.contains kc
      | none => false
  (innerAB as bs).flatMap fun (a, b) =>
    csPruned.filterMap fun c =>
      match b.k, c.k with
      | some kb, some kc => if kb == kc then some (a.id, b.id, c.id) else none
      | _, _ => none

def refLeft3 (as : List RowA) (bs : List RowB) (cs : List RowC) :
    List (Nat × Option Nat × Nat) :=
  (leftAB as bs).flatMap fun (a, ob) =>
    cs.filterMap fun c =>
      match a.k, ob.bind RowB.k, c.k with
      | some ka, some kb, some kc =>
          if kb == kc && ka == kc then some (a.id, ob.map RowB.id, c.id) else none
      | _, _, _ => none

/-- Incorrectly dropping `a.k = c.k` (or `b.k = c.k`) across an outer join
    boundary `(A ⟕ B) ⋈ C` when `b.k = c.k` can become NULL! -/
def buggyDropAcrossLeftJoin (as : List RowA) (bs : List RowB) (cs : List RowC) :
    List (Nat × Option Nat × Nat) :=
  (leftAB as bs).flatMap fun (a, ob) =>
    cs.filterMap fun c =>
      match a.k, c.k with
      | some ka, some kc =>
          if ka == kc then some (a.id, ob.map RowB.id, c.id) else none
      | _, _ => none

/-- Transitive equivalence key closure, semijoin pushdown, and duplicate hash-key
    dropping (JOB J5 / STATS-CEB C1, C5 / LDBC L1, L2 / JOB J4): for inner joins
    `A ⋈_{a.k = b.k} B ⋈_{b.k = c.k ∧ a.k = c.k} C`, pushing `A`'s key domain into
    `C` and eliding the redundant `a.k = c.k` comparison (`keys=2 -> keys=1`) is
    unconditionally sound, whereas dropping the `B`-side check across a
    `LEFT OUTER JOIN` (`A ⟕ B`) changes results. -/
def Goal : Prop :=
  (∀ (as : List RowA) (bs : List RowB) (cs : List RowC),
    optInner3 as bs cs = refInner3 as bs cs) ∧
  (∃ (as : List RowA) (bs : List RowB) (cs : List RowC),
    buggyDropAcrossLeftJoin as bs cs ≠ refLeft3 as bs cs)

example :
    let as : List RowA := [{ id := 1, k := some 10 }, { id := 2, k := none }, { id := 3, k := some 20 }]
    let bs : List RowB := [{ id := 10, k := some 10 }, { id := 11, k := some 30 }]
    let cs : List RowC := [{ id := 100, k := some 10 }, { id := 101, k := some 30 }, { id := 102, k := none }]
    optInner3 as bs cs = refInner3 as bs cs ∧
    optInner3 as bs cs = [(1, 10, 100)] := by
  decide

example :
    let as : List RowA := [{ id := 1, k := some 10 }]
    let bs : List RowB := [] -- unmatched LEFT JOIN yields ob = none
    let cs : List RowC := [{ id := 100, k := some 10 }]
    buggyDropAcrossLeftJoin as bs cs = [(1, none, 100)] ∧
    refLeft3 as bs cs = [] := by
  decide

end TinylambOpt.ResearchGoals.TransitiveEquivPredicatePushdownAndDupKeyDrop
