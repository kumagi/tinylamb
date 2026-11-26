import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_CardinalityErrorRegret

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  Cardinality-error regret: for linear per-row billing modes, if a mode wins at
  the estimated row count, its loss at the actual row count is bounded by the
  rate gap times the estimation error.  The proof splits on the two rate orders
  and the two directions of the error; SQL execution may keep the estimated
  winner with that certified regret bound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.CardinalityErrorRegret.Goal := by
  intro a b estimate actual hwin
  unfold bill at hwin ⊢
  rw [Nat.sub_le_iff_le_add']
  by_cases hp : a.perRow ≤ b.perRow
  · have hmaxrate : max a.perRow b.perRow = b.perRow := Nat.max_eq_right hp
    have hminrate : min a.perRow b.perRow = a.perRow := Nat.min_eq_left hp
    by_cases hxa : actual ≤ estimate
    · have hmaxest : max estimate actual = estimate := Nat.max_eq_left hxa
      have hminest : min estimate actual = actual := Nat.min_eq_right hxa
      rw [hmaxrate, hminrate, hmaxest, hminest]
      let d := b.perRow - a.perRow
      let f := estimate - actual
      change a.setup + a.perRow * actual ≤ b.setup + b.perRow * actual + d * f
      have hpb : b.perRow = a.perRow + d := by dsimp [d]; omega
      have he : estimate = actual + f := by dsimp [f]; omega
      rw [hpb] at hwin ⊢
      rw [he] at hwin
      simp only [Nat.add_mul, Nat.add_assoc, Nat.left_distrib] at hwin ⊢
      omega
    · have hea : estimate ≤ actual := by omega
      have hmaxest : max estimate actual = actual := Nat.max_eq_right hea
      have hminest : min estimate actual = estimate := Nat.min_eq_left hea
      rw [hmaxrate, hminrate, hmaxest, hminest]
      let d := b.perRow - a.perRow
      let f := actual - estimate
      change a.setup + a.perRow * actual ≤ b.setup + b.perRow * actual + d * f
      have hpb : b.perRow = a.perRow + d := by dsimp [d]; omega
      have he : actual = estimate + f := by dsimp [f]; omega
      rw [hpb] at hwin ⊢
      rw [he]
      simp only [Nat.add_mul, Nat.add_assoc, Nat.left_distrib] at hwin ⊢
      omega
  · have hbp : b.perRow ≤ a.perRow := by omega
    have hmaxrate : max a.perRow b.perRow = a.perRow := Nat.max_eq_left hbp
    have hminrate : min a.perRow b.perRow = b.perRow := Nat.min_eq_right hbp
    by_cases hea : estimate ≤ actual
    · have hmaxest : max estimate actual = actual := Nat.max_eq_right hea
      have hminest : min estimate actual = estimate := Nat.min_eq_left hea
      rw [hmaxrate, hminrate, hmaxest, hminest]
      let d := a.perRow - b.perRow
      let f := actual - estimate
      change a.setup + a.perRow * actual ≤ b.setup + b.perRow * actual + d * f
      have hpa : a.perRow = b.perRow + d := by dsimp [d]; omega
      have he : actual = estimate + f := by dsimp [f]; omega
      rw [hpa] at hwin ⊢
      rw [he]
      simp only [Nat.add_mul, Nat.add_assoc, Nat.left_distrib] at hwin ⊢
      omega
    · have hxa : actual ≤ estimate := by omega
      have hmaxest : max estimate actual = estimate := Nat.max_eq_left hxa
      have hminest : min estimate actual = actual := Nat.min_eq_right hxa
      rw [hmaxrate, hminrate, hmaxest, hminest]
      let d := a.perRow - b.perRow
      let f := estimate - actual
      change a.setup + a.perRow * actual ≤ b.setup + b.perRow * actual + d * f
      have hpa : a.perRow = b.perRow + d := by dsimp [d]; omega
      have he : estimate = actual + f := by dsimp [f]; omega
      rw [hpa] at hwin ⊢
      rw [he] at hwin
      simp only [Nat.add_mul, Nat.add_assoc, Nat.left_distrib] at hwin ⊢
      omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_CardinalityErrorRegret
