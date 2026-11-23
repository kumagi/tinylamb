import Std

namespace TinylambOpt.ProposedGoals.QualifyPushdown

/-- (partition key, measure, payload) -/
abbrev Row := Int × Int × Nat

def partSum (rows : List Row) (k : Int) : Int :=
  (rows.filter (fun r => r.1 == k)).map (fun r => r.2.1) |>.sum

/-- QUALIFY is applied after the window value is computed. -/
def qualify (q : Int → Bool) (rows : List Row) : List Row :=
  rows.filter (fun r => q (partSum rows r.1))

def whereFilter (w : Row → Bool) (rows : List Row) : List Row :=
  rows.filter w

/-- A predicate depending only on the partition key is constant on each
    partition, so it commutes with windowing and QUALIFY. -/
def PartitionOnly (w : Row → Bool) : Prop :=
  ∀ a b : Row, a.1 == b.1 → w a = w b

def Goal : Prop :=
  ∀ (w : Row → Bool) (q : Int → Bool) (rows : List Row),
    PartitionOnly w →
      whereFilter w (qualify q rows) = qualify q (whereFilter w rows)

/-- Fixture: a predicate on the measure itself is NOT partition-only and
    changes the window total when pushed down. -/
def badW : Row → Bool := fun r => r.2.1 == 1
def rowsEx : List Row := [(1, 1, 0), (1, 2, 1)]

example : whereFilter badW (qualify (fun s => s > 2) rowsEx) = [(1, 1, 0)] := by decide
example : qualify (fun s => s > 2) (whereFilter badW rowsEx) = [] := by decide
example : ¬ PartitionOnly badW := by
  intro h
  have heq := h (1, 1, 0) (1, 2, 1) (by decide)
  simp [badW] at heq

end TinylambOpt.ProposedGoals.QualifyPushdown
