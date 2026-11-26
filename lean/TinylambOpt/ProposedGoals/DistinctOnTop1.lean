import Std

/-!
(group key, ORDER BY key, payload); input order breaks ties.

Streaming plan: keep only the best row per key while scanning once.

Modeled claims

- `Goal` — The one-pass top-1 state equals sorting and dropping duplicates, for every
  key including NULL and for tied ORDER BY values.
-/

namespace TinylambOpt.ProposedGoals.DistinctOnTop1

/-- (group key, ORDER BY key, payload); input order breaks ties. -/
abbrev Row := Option Int × Int × Nat

def keyOf (r : Row) : Option Int := r.1

def beforeRow (a b : Row × Nat) : Bool :=
  a.1.2.1 < b.1.2.1 || (a.1.2.1 == b.1.2.1 && a.2 < b.2)

/-- Streaming plan: keep only the best row per key while scanning once. -/
def top1Fold (rows : List Row) : Option Int → Option Row :=
  fun k =>
    (rows.zipIdx.foldl (fun acc ir =>
      if keyOf ir.1 != k then acc else
        match acc with
        | none => some ir
        | some best => if beforeRow ir best then some ir else acc) none).map Prod.fst

/-- Reference plan: stable sort by (ORDER BY key, input position), then take
    the first row of each distinct key (NULL keys form one group). -/
def sortDedup (rows : List Row) : Option Int → Option Row :=
  let sorted := rows.zipIdx.mergeSort beforeRow
  fun k => (sorted.find? (fun ir => keyOf ir.1 == k)).map Prod.fst

def distinctOn (rows : List Row) : List Row :=
  (rows.map keyOf).filterMap (top1Fold rows) |>.eraseDups

/-- The one-pass top-1 state equals sorting and dropping duplicates, for every
    key including NULL and for tied ORDER BY values. -/
def Goal : Prop := ∀ (rows : List Row) (k : Option Int),
  top1Fold rows k = sortDedup rows k

example : top1Fold [(none, 2, 1), (some 1, 5, 2), (none, 1, 3)] none = some (none, 1, 3) := by native_decide
example : sortDedup [(none, 2, 1), (some 1, 5, 2), (none, 1, 3)] none = some (none, 1, 3) := by native_decide
example : top1Fold [(some 1, 1, 10), (some 1, 1, 11)] (some 1) = some (some 1, 1, 10) := by native_decide
example : distinctOn [(some 1, 1, 10), (some 1, 1, 11)] = [(some 1, 1, 10)] := by native_decide

end TinylambOpt.ProposedGoals.DistinctOnTop1
