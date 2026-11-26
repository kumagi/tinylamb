import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_RowGoalEndpointDominance
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans RowGoalEndpointDominance
theorem sound : Goal := by
  intro a b n k hn haw hbw has hww
  have hn0 : n ≠ 0 := Nat.pos_iff_ne_zero.mp hn
  simp only [latency, hn0, ite_false]
  let m := min n k
  have hmn : m ≤ n := Nat.min_le_left _ _
  have hcross : a.startup * n + (a.work - a.startup) * m ≤ b.startup * n + (b.work - b.startup) * m := by
    have ha : a.startup * n + (a.work - a.startup) * m = a.startup * (n - m) + a.work * m := by
      have h1 : a.startup * n = a.startup * (n - m) + a.startup * m := by
        rw [← Nat.mul_add, Nat.sub_add_cancel hmn]
      have hasm : a.startup * m ≤ a.work * m := Nat.mul_le_mul_right m haw
      rw [h1, Nat.sub_mul]
      omega
    have hb : b.startup * n + (b.work - b.startup) * m = b.startup * (n - m) + b.work * m := by
      have h1 : b.startup * n = b.startup * (n - m) + b.startup * m := by
        rw [← Nat.mul_add, Nat.sub_add_cancel hmn]
      have hbsm : b.startup * m ≤ b.work * m := Nat.mul_le_mul_right m hbw
      rw [h1, Nat.sub_mul]
      omega
    rw [ha, hb]
    have h2 : a.startup * (n - m) ≤ b.startup * (n - m) := Nat.mul_le_mul_right _ has
    have h3 : a.work * m ≤ b.work * m := Nat.mul_le_mul_right _ hww
    omega
  have hdiv := Nat.div_le_div_right (c := n) hcross
  have hsimp_a : (a.startup * n + (a.work - a.startup) * m) / n = a.startup + (a.work - a.startup) * m / n := by
    rw [Nat.mul_comm a.startup n, Nat.mul_add_div hn a.startup ((a.work - a.startup) * m)]
  have hsimp_b : (b.startup * n + (b.work - b.startup) * m) / n = b.startup + (b.work - b.startup) * m / n := by
    rw [Nat.mul_comm b.startup n, Nat.mul_add_div hn b.startup ((b.work - b.startup) * m)]
  rw [hsimp_a, hsimp_b] at hdiv
  exact hdiv
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_RowGoalEndpointDominance
