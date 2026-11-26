import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# FunctionalWarehouseAttributesAfterMonthlySum

When warehouse attributes are fixed by the warehouse key, attaching them after the
monthly sum preserves the grouped totals.

The hash aggregate can use a narrower key and look up the constant attributes on its
output.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.FunctionalWarehouseAttributesAfterMonthlySum
open TinylambOpt.ResearchGoals.QueryRewrites

theorem key_eq_iff (attrs : Nat → String) (id month w m : Nat) :
    (((id, attrs id, month) : Nat × String × Nat) == (w, attrs w, m)) =
    (((id, month) : Nat × Nat) == (w, m)) := by
  by_cases hid : id == w
  · have hidEq : id = w := beq_iff_eq.mp hid
    subst id
    simp [BEq.beq]
  · have hidne : id ≠ w := by
      intro h
      apply hid
      simp [h]
    simp [BEq.beq, hidne]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.FunctionalWarehouseAttributesAfterMonthlySum.Goal := by
  intro facts attrs w m
  dsimp
  unfold forKey
  induction facts with
  | nil => rfl
  | cons f fs ih =>
    rcases f with ⟨⟨id, month⟩, v⟩
    simp only [List.map_cons, List.filter_cons]
    rw [key_eq_iff attrs id month w m]
    by_cases hsame : id = w ∧ month = m
    · simp [hsame, ih]
    · simp [hsame, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.FunctionalWarehouseAttributesAfterMonthlySum
