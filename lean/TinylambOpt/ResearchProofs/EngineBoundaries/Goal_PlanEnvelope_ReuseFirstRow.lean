import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

/-!
A cached or specialized plan needs the modeled validity conditions before it can serve a
SQL invocation.

Refuted by counterexample

- `ReuseFirstRow` — Aggressive memo reuse: the LIMIT 1 winner remains a winner at every
  larger row count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_ReuseFirstRow

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem cost_add (p : Plan) (lo n g : Nat) (hlo : lo ≤ n) :
    cost p n g = cost p lo g + p.perRow * (n - lo) := by
  unfold cost
  have hmul : p.perRow * lo + p.perRow * (n - lo) = p.perRow * n := by
    rw [← Nat.mul_add, Nat.add_sub_of_le hlo]
  omega

theorem refute : ¬ ReuseFirstRow.Goal := by
  intro h
  let lowStart : Plan := ⟨0,8,0,0⟩
  let lowSlope : Plan := ⟨12,1,0,0⟩
  have hbest : best [lowStart, lowSlope] 1 0 = some (cost lowStart 1 0) := by native_decide
  have hh := h [lowStart, lowSlope] lowStart 0 8 (by simp) (by decide) hbest
  have hbad : ¬ best [lowStart, lowSlope] 8 0 = some (cost lowStart 8 0) := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_ReuseFirstRow
