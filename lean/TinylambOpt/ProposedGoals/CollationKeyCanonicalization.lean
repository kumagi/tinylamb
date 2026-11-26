import Std

/-!
Values are byte strings; a collation maps them to canonical sort keys.

The key must not merge two distinct raw values (no over-grouping). Congruence in the
other direction is automatic because `key` is a function.

Modeled claims

- `Goal` — Key-based grouping and ordering may replace raw grouping and ordering as
  long as the key does not merge distinct raw values and preserves order.
-/

namespace TinylambOpt.ProposedGoals.CollationKeyCanonicalization

/-- Values are byte strings; a collation maps them to canonical sort keys. -/
abbrev Val := List Nat
abbrev Row := Val × Nat -- (value, measure to aggregate)

def rawEq (a b : Val) : Bool := a == b

def groupRaw (rows : List Row) (v : Val) : Nat :=
  (rows.filter (fun r => rawEq r.1 v)).map Prod.snd |>.sum

def groupByKey (key : Val → List Nat) (rows : List Row) (v : Val) : Nat :=
  (rows.filter (fun r => key r.1 == key v)).map Prod.snd |>.sum

def orderRaw (a b : Val) : Bool := a < b
def orderKey (key : Val → List Nat) (a b : Val) : Bool := key a < key b

/-- The key must not merge two distinct raw values (no over-grouping).
    Congruence in the other direction is automatic because `key` is a function. -/
def KeyInject (key : Val → List Nat) : Prop :=
  ∀ a b, key a == key b → rawEq a b

/-- Key-based grouping and ordering may replace raw grouping and ordering as
    long as the key does not merge distinct raw values and preserves order. -/
def Goal : Prop :=
  ∀ (key : Val → List Nat), KeyInject key →
    (∀ a b, orderRaw a b = orderKey key a b) →
    ∀ (rows : List Row) (v : Val),
      groupByKey key rows v = groupRaw rows v

/-- Fixture: a constant key merges distinct values (the guard rejects it). -/
def badKey : Val → List Nat := fun _ => []
example : groupByKey badKey [( [1], 1), ([2], 2)] [1] = 3 := by decide
example : groupRaw [([1], 1), ([2], 2)] [1] = 1 := by decide

example : groupByKey id [( [1], 1), ([1], 2), ([2], 4)] [1] = 3 := by decide

end TinylambOpt.ProposedGoals.CollationKeyCanonicalization
