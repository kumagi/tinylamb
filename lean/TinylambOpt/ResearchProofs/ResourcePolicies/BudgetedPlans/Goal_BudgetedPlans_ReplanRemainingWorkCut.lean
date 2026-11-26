import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_ReplanRemainingWorkCut
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans ReplanRemainingWorkCut
theorem sound : Goal := by
  intro sunk conversion remaining oldRate newRate hle
  have hmul : remaining * (oldRate - newRate) = remaining * oldRate - remaining * newRate :=
    Nat.mul_sub remaining oldRate newRate
  have hle2 : remaining * newRate ≤ remaining * oldRate := Nat.mul_le_mul_left remaining hle
  constructor
  · intro h
    have h' : conversion + remaining * newRate ≤ remaining * oldRate := by omega
    rw [hmul]
    omega
  · intro h
    rw [hmul] at h
    omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_ReplanRemainingWorkCut
