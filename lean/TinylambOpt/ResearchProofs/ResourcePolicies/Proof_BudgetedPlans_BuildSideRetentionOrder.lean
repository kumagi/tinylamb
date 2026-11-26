import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_BuildSideRetentionOrder

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  Build-side retention order: swapping two build inputs is safe for the blocking
  join peak when the excess peak above retained output of the new right input is
  no larger than that of the old right input.  The proof compares the three
  maxima in the `join` profile; SQL execution may swap build sides under this
  excess-peak condition without raising the modeled retained peak.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.BuildSideRetentionOrder.Goal := by
  intro a b s ha hb hexc
  simp [join]
  rw [Nat.max_le]
  constructor
  · exact Nat.le_trans (by omega : a.peak ≤ b.held + a.peak)
      (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_right _ _))
  · rw [Nat.max_le]
    constructor
    · have hbEq : b.peak = b.held + (b.peak - b.held) := by omega
      have haEq : a.peak = a.held + (a.peak - a.held) := by omega
      rw [hbEq, haEq]
      omega
    · omega
end TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_BuildSideRetentionOrder
