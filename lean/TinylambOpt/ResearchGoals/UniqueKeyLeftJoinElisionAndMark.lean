import Std

/-!
These claims model removing an unused left-join input or replacing it with an existence
mark under uniqueness assumptions. SQL must preserve all left rows and any observable
match information.

Modeled claims

- `Goal` — Unique-key `LEFT JOIN` elision, 1-bit Mark reduction, and non-equi `<`
  upper-bound semijoin pushdown (TPC-DS Q40/Q72/Q80): (1) when right non-null keys
  are unique, projecting only left columns elides the `LEFT JOIN`, (2) testing `r.key
  IS NOT NULL` reduces to a key-membership 1-bit mark without reading right payloads,
  and (3) `l.val < r.val` allows pre-filtering `L` by `l.val < max(r.val)`.
-/

namespace TinylambOpt.ResearchGoals.UniqueKeyLeftJoinElisionAndMark

abbrev LRow := Option Int × Int
abbrev RRow := Option Int × Int

def ljoin (ls : List LRow) (rs : List RRow) : List (LRow × Option RRow) :=
  ls.flatMap fun l =>
    let hits :=
      match l.1 with
      | none => []
      | some lk =>
          rs.filterMap fun (r : RRow) =>
            match r.1 with
            | some rk => if lk == rk then some r else none
            | none => none
    if hits.isEmpty then [(l, none)]
    else hits.map (fun r => (l, some r))

def NonNullKeysNodup (rs : List RRow) : Prop :=
  (rs.filterMap Prod.fst).Nodup

def optLeftOnlyElide (ls : List LRow) : List LRow := ls

def memOptKey (k : Option Int) (rs : List RRow) : Bool :=
  match k with
  | none => false
  | some lk => (rs.filterMap Prod.fst).contains lk

def optMarkJoin (ls : List LRow) (rs : List RRow) : List (Int × Bool) :=
  ls.map (fun l => (l.2, memOptKey l.1 rs))

def innerJoinLt (ls : List LRow) (rs : List RRow) : List (LRow × RRow) :=
  ls.flatMap fun l =>
    rs.filterMap fun r =>
      match l.1, r.1 with
      | some lk, some rk =>
          if lk == rk && decide (l.2 < r.2) then some (l, r) else none
      | _, _ => none

def dupKeyR : List RRow := [(some 1, 10), (some 1, 20)]

/-- Unique-key `LEFT JOIN` elision, 1-bit Mark reduction, and non-equi `<`
    upper-bound semijoin pushdown (TPC-DS Q40/Q72/Q80):
    (1) when right non-null keys are unique, projecting only left columns
    elides the `LEFT JOIN`, (2) testing `r.key IS NOT NULL` reduces to a
    key-membership 1-bit mark without reading right payloads, and
    (3) `l.val < r.val` allows pre-filtering `L` by `l.val < max(r.val)`. -/
def Goal : Prop :=
  (∀ (ls : List LRow) (rs : List RRow),
    NonNullKeysNodup rs →
      (ljoin ls rs).map Prod.fst = optLeftOnlyElide ls ∧
      (ljoin ls rs).map (fun (l, ro) => (l.2, ro.isSome)) = optMarkJoin ls rs) ∧
  (∀ (ls : List LRow) (rs : List RRow) (maxR : Int),
    (∀ r ∈ rs, r.2 ≤ maxR) →
      innerJoinLt (ls.filter (fun l => decide (l.2 < maxR))) rs = innerJoinLt ls rs) ∧
  ((ljoin [(some 1, 5)] dupKeyR).map Prod.fst ≠ optLeftOnlyElide [(some 1, 5)])

example :
    let ls : List LRow := [(none, 1), (some 1, 2), (some 2, 3)]
    let rs : List RRow := [(none, 99), (some 1, 10)]
    (ljoin ls rs).map Prod.fst = optLeftOnlyElide ls ∧
    (ljoin ls rs).map (fun (l, ro) => (l.2, ro.isSome)) = optMarkJoin ls rs := by
  decide

example :
    let ls : List LRow := [(some 1, 5), (some 1, 20)]
    let rs : List RRow := [(some 1, 10), (some 1, 15)]
    innerJoinLt (ls.filter (fun l => decide (l.2 < 15))) rs = innerJoinLt ls rs ∧
    (ljoin [(some 1, 5)] dupKeyR).map Prod.fst ≠ optLeftOnlyElide [(some 1, 5)] := by
  decide

end TinylambOpt.ResearchGoals.UniqueKeyLeftJoinElisionAndMark
