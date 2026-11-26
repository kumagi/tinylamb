import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_RowGoalEndpointDominance

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  Row-goal endpoint dominance: a plan no worse at startup and at total work
  keeps no worse interpolated latency for every positive row count and every
  requested fraction.  The proof bounds the interpolated numerator by the startup gap
  plus the rival numerator, then applies monotonicity of natural division.  SQL
  execution may reuse the dominant plan for any intermediate LIMIT demand.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.RowGoalEndpointDominance.Goal := by
  intro a b n k hn hsa hsb hs hw
  unfold latency
  simp [show n ≠ 0 by omega]
  let r := min n k
  have hr : r ≤ n := by exact Nat.min_le_left n k
  have hDa : a.work - a.startup ≤ (b.startup - a.startup) + (b.work - b.startup) := by
    have h1 : a.startup + (a.work - a.startup) = a.work := by omega
    have h2 : a.startup + (b.startup - a.startup) = b.startup := by omega
    have h3 : b.startup + (b.work - b.startup) = b.work := by omega
    omega
  have hmul : (a.work - a.startup) * r ≤
      (b.startup - a.startup) * n + (b.work - b.startup) * r := by
    have h1 : (a.work - a.startup) * r ≤
        ((b.startup - a.startup) + (b.work - b.startup)) * r :=
      Nat.mul_le_mul_right r hDa
    have h2 : ((b.startup - a.startup) + (b.work - b.startup)) * r =
        (b.startup - a.startup) * r + (b.work - b.startup) * r := by
      rw [Nat.add_mul]
    have h3 : (b.startup - a.startup) * r ≤ (b.startup - a.startup) * n :=
      Nat.mul_le_mul_left (b.startup - a.startup) hr
    omega
  have hdiv : (a.work - a.startup) * r / n ≤
      (b.startup - a.startup) + (b.work - b.startup) * r / n := by
    calc
      (a.work - a.startup) * r / n ≤
          ((b.startup - a.startup) * n + (b.work - b.startup) * r) / n :=
        Nat.div_le_div_right hmul
      _ = (b.startup - a.startup) + (b.work - b.startup) * r / n := by
        rw [Nat.mul_comm (b.startup - a.startup) n]
        exact Nat.mul_add_div hn (b.startup - a.startup) ((b.work - b.startup) * r)
  calc
    a.startup + (a.work - a.startup) * min n k / n ≤
        a.startup + ((b.startup - a.startup) + (b.work - b.startup) * r / n) := by
      simpa [r] using Nat.add_le_add_left hdiv a.startup
    _ = b.startup + (b.work - b.startup) * min n k / n := by
      simp [r]
      omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_RowGoalEndpointDominance
