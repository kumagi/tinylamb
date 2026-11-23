import Std

namespace TinylambOpt.ResearchGoals.CanonicalEquivIdSetAlgebra

def insertUniq {α : Type} [BEq α] (xs : List α) (x : α) : List α :=
  if xs.contains x then xs else xs ++ [x]

def dedupList {α : Type} [BEq α] (xs : List α) : List α :=
  xs.foldl insertUniq []

def setIntersect {α : Type} [BEq α] (a b c : List α) : List α :=
  (dedupList a).filter (fun x => b.contains x && c.contains x)

def setExcept {α : Type} [BEq α] (a b c : List α) : List α :=
  (dedupList a).filter (fun x => !b.contains x && !c.contains x)

abbrev DimRow := Nat × Option String
abbrev FactRow := Nat × Nat  -- (surrogate_key, date_id)

def joinProjectAttrDate (f : List FactRow) (d : List DimRow) :
    List (Option String × Nat) :=
  f.flatMap fun (sk, dt) =>
    d.filterMap fun (dsk, attr) =>
      if sk == dsk then some (attr, dt) else none

def refIntersectCount (d : List DimRow) (f1 f2 f3 : List FactRow) : Nat :=
  let s1 := joinProjectAttrDate f1 d
  let s2 := joinProjectAttrDate f2 d
  let s3 := joinProjectAttrDate f3 d
  (setIntersect s1 s2 s3).length

def refExceptCount (d : List DimRow) (f1 f2 f3 : List FactRow) : Nat :=
  let s1 := joinProjectAttrDate f1 d
  let s2 := joinProjectAttrDate f2 d
  let s3 := joinProjectAttrDate f3 d
  (setExcept s1 s2 s3).length

def toCanonPairs (d : List DimRow) (canon : Option String → Nat)
    (f : List FactRow) : List (Nat × Nat) :=
  f.flatMap fun (sk, dt) =>
    d.filterMap fun (dsk, attr) =>
      if sk == dsk then some (canon attr, dt) else none

/-- Probe smallest set first (`f3`, then `f2`, then `f1`) in integer canonical
    equivalence-ID space for `INTERSECT DISTINCT`. -/
def optCanonIntersectCount (d : List DimRow) (canon : Option String → Nat)
    (f1 f2 f3 : List FactRow) : Nat :=
  let c3 := dedupList (toCanonPairs d canon f3)
  let c2 := dedupList ((toCanonPairs d canon f2).filter (fun p => c3.contains p))
  let c1 := dedupList ((toCanonPairs d canon f1).filter (fun p => c2.contains p))
  c1.length

/-- Single-table bitmask/filter deletion in canonical equivalence-ID space for
    `(f1 \ f2) \ f3` (`EXCEPT DISTINCT`). -/
def optCanonExceptCount (d : List DimRow) (canon : Option String → Nat)
    (f1 f2 f3 : List FactRow) : Nat :=
  let c2 := dedupList (toCanonPairs d canon f2)
  let c3 := dedupList (toCanonPairs d canon f3)
  let c1 := dedupList (toCanonPairs d canon f1)
  (c1.filter (fun p => !c2.contains p && !c3.contains p)).length

/-- Surrogate-key naive `INTERSECT` skips canonicalization when two distinct
    surrogate keys share the same attribute (`(1, "Smith")` vs `(2, "Smith")`). -/
def badSurrogateIntersectCount (f1 f2 f3 : List FactRow) : Nat :=
  (setIntersect f1 f2 f3).length

/-- Canonical integer equivalence-ID set algebra for `INTERSECT` / `EXCEPT`
    (TPC-DS Q38/Q87/Q14): mapping projected join attributes via an injective
    canonical ID preserves exact `INTERSECT` and `EXCEPT` cardinalities under
    smallest-channel probing and direct deletion, whereas comparing raw
    surrogate keys is unsound when multiple SKs share the same attribute. -/
def Goal : Prop :=
  (∀ (d : List DimRow) (canon : Option String → Nat) (f1 f2 f3 : List FactRow),
    (∀ a b, canon a = canon b ↔ a = b) →
      optCanonIntersectCount d canon f1 f2 f3 = refIntersectCount d f1 f2 f3 ∧
      optCanonExceptCount d canon f1 f2 f3 = refExceptCount d f1 f2 f3) ∧
  (∃ (d : List DimRow) (f1 f2 f3 : List FactRow),
    badSurrogateIntersectCount f1 f2 f3 ≠ refIntersectCount d f1 f2 f3)

private def dupNameDim : List DimRow :=
  [(1, some "Smith"), (2, some "Smith"), (3, none)]
private def testCanon : Option String → Nat
  | none => 0
  | some "Smith" => 1
  | some "Jones" => 2
  | some s => s.length + 10

example :
    let f1 : List FactRow := [(1, 100), (3, 200), (1, 100)]
    let f2 : List FactRow := [(2, 100)]
    let f3 : List FactRow := [(2, 100)]
    optCanonIntersectCount dupNameDim testCanon f1 f2 f3 = 1 ∧
    refIntersectCount dupNameDim f1 f2 f3 = 1 ∧
    badSurrogateIntersectCount f1 f2 f3 = 0 ∧
    optCanonExceptCount dupNameDim testCanon f1 f2 f3 = 1 ∧
    refExceptCount dupNameDim f1 f2 f3 = 1 := by
  decide

example :
    let f1 : List FactRow := [(1, 100), (3, 200)]
    let f2 : List FactRow := [(2, 100), (3, 200)]
    let f3 : List FactRow := [(2, 100)]
    optCanonExceptCount dupNameDim testCanon f1 f2 f3 = 0 ∧
    refExceptCount dupNameDim f1 f2 f3 = 0 := by
  decide

end TinylambOpt.ResearchGoals.CanonicalEquivIdSetAlgebra
