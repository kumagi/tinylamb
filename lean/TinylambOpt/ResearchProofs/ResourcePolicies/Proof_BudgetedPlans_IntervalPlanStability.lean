import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_IntervalPlanStability

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  Interval plan stability: an affine bill that is no worse at both endpoints of
  a cardinality interval is no worse at every point inside it.  The proof uses
  the sign of the rate difference to pick the appropriate endpoint and cancels
  the common prefix of work.  SQL execution may share one memo entry across
  such a bounded interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.IntervalPlanStability.Goal := by
  intro a b lo hi n hlo hn hloBill hhiBill
  unfold bill at hloBill hhiBill ⊢
  by_cases hp : a.perRow ≤ b.perRow
  · have hnl : n = lo + (n - lo) := by omega
    have hinc : a.perRow * (n - lo) ≤ b.perRow * (n - lo) :=
      Nat.mul_le_mul_right (n - lo) hp
    rw [hnl]
    simp only [Nat.mul_add]
    omega
  · have hpb : b.perRow ≤ a.perRow := by omega
    have hni : hi = n + (hi - n) := by omega
    have hinc : b.perRow * (hi - n) ≤ a.perRow * (hi - n) :=
      Nat.mul_le_mul_right (hi - n) hpb
    rw [hni] at hhiBill
    simp only [Nat.mul_add] at hhiBill
    omega
end TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_IntervalPlanStability
