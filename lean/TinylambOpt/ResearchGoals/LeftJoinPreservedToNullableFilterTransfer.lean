import Std

/-!
Counterexample showing filtering the nullable side `R` on a non-join column turns
matched rows into spurious NULL-padded rows.

Modeled claims

- `Goal` — Left-join preserved-side join-key filter transfer (TPC-DS Q78) and `UNION
  DISTINCT` key-filter pushdown (TPC-DS Q75): (1) filtering nullable right table `R`
  by a predicate on the join key already implied by preserved side `L` produces the
  exact same `LEFT JOIN` output, whereas (2) filtering `R` on a non-join column
  breaks semantics, and (3) a filter on `UNION DISTINCT` output commutes into both
  inputs.
-/

namespace TinylambOpt.ResearchGoals.LeftJoinPreservedToNullableFilterTransfer

abbrev Row := Option Int × Int  -- (join_key, payload)

def liftOpt (p : Int → Bool) : Option Int → Bool
  | none => false
  | some k => p k

def ljoin (ls rs : List Row) : List (Row × Option Row) :=
  ls.flatMap fun l =>
    let hits :=
      match l.1 with
      | none => []
      | some lk =>
          rs.filterMap fun (r : Row) =>
            match r.1 with
            | some rk => if lk == rk then some r else none
            | none => none
    if hits.isEmpty then [(l, none)]
    else hits.map (fun r => (l, some r))

def insertUniq {α : Type} [BEq α] (xs : List α) (x : α) : List α :=
  if xs.contains x then xs else xs ++ [x]

def unionDistinct {α : Type} [BEq α] (a b : List α) : List α :=
  (a ++ b).foldl insertUniq []

/-- Counterexample showing filtering the nullable side `R` on a non-join column
    turns matched rows into spurious NULL-padded rows. -/
theorem badFilterRPayload :
    let ls : List Row := [(some 1, 100)]
    let rs : List Row := [(some 1, 50)]
    ljoin ls (rs.filter (fun r => decide (r.2 > 99))) ≠ ljoin ls rs := by
  decide

/-- Left-join preserved-side join-key filter transfer (TPC-DS Q78) and
    `UNION DISTINCT` key-filter pushdown (TPC-DS Q75):
    (1) filtering nullable right table `R` by a predicate on the join key
    already implied by preserved side `L` produces the exact same `LEFT JOIN`
    output, whereas (2) filtering `R` on a non-join column breaks semantics,
    and (3) a filter on `UNION DISTINCT` output commutes into both inputs. -/
def Goal : Prop :=
  (∀ (ls rs : List Row) (p : Int → Bool),
    let lsFiltered := ls.filter (fun l => liftOpt p l.1)
    let rsFiltered := rs.filter (fun r => liftOpt p r.1)
    ljoin lsFiltered rsFiltered = ljoin lsFiltered rs) ∧
  (∀ {α : Type} [BEq α] [LawfulBEq α] (a b : List α) (p : α → Bool),
    (unionDistinct a b).filter p = unionDistinct (a.filter p) (b.filter p)) ∧
  (∃ (ls rs : List Row) (q : Int → Bool),
    ljoin ls (rs.filter (fun r => q r.2)) ≠ ljoin ls rs)

example :
    let ls : List Row := [(none, 1), (some 1, 2), (some 2, 3), (some 3, 4)]
    let rs : List Row := [(none, 9), (some 1, 10), (some 2, 20), (some 2, 21)]
    let p : Int → Bool := fun k => k != 1 && k != 9
    ljoin (ls.filter (fun l => liftOpt p l.1)) (rs.filter (fun r => liftOpt p r.1)) =
      ljoin (ls.filter (fun l => liftOpt p l.1)) rs := by
  decide

example :
    let a : List Int := [1, 2, 1, 3]
    let b : List Int := [2, 4, 3, 5]
    let p : Int → Bool := fun x => x % 2 == 1
    (unionDistinct a b).filter p = unionDistinct (a.filter p) (b.filter p) := by
  decide

end TinylambOpt.ResearchGoals.LeftJoinPreservedToNullableFilterTransfer
