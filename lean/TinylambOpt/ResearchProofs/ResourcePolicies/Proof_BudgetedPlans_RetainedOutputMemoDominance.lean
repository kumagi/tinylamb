import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_RetainedOutputMemoDominance

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  Retained-output memo dominance: componentwise dominance of build profiles is
  preserved after embedding in a blocking join, including work, startup, peak,
  and retained output.  The proof checks each component of `join`; SQL execution
  may prune the dominated build candidate while the probe stays live.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.RetainedOutputMemoDominance.Goal := by
  intro a b probe scratch hab
  rcases hab with ⟨hw, hs, hp, hh⟩
  constructor
  · simp [join]
    omega
  constructor
  · simp [join]
    omega
  constructor
  · simp [join]
    rw [Nat.max_le]
    constructor
    · exact Nat.le_trans hp (Nat.le_max_left _ _)
    · rw [Nat.max_le]
      constructor <;> omega
  · simp [join]
    omega
end TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_RetainedOutputMemoDominance
