import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
The regret of picking the estimated winner is bounded by (slope gap) · (cardinality
error). The setup-cost gap is controlled at the estimate endpoint; moving the demand by
`|estimate − actual|` rows changes the bill difference by at most the slope gap times
that distance. Four cases (slope order × cardinality order) cover everything; when the
actual endpoint is the cheaper one the regret truncates to 0. Product-decomposition
facts keep `omega` on linear atoms. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_CardinalityErrorRegret
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

/-- The steeper slope's bill decomposes into the shallower slope's bill plus the gap. -/
private theorem slope_split (h : p ≤ q) (x : Nat) : q * x = p * x + (q - p) * x := by
  induction x with
  | zero => simp
  | succ x ih => rw [Nat.mul_succ, Nat.mul_succ, Nat.mul_succ]; omega

theorem sound : CardinalityErrorRegret.Goal := by
  intro a b estimate actual hest
  simp only [bill] at hest ⊢
  rcases Nat.le_total a.perRow b.perRow with hpq | hpq
  · -- b.perRow is the steeper slope
    rcases Nat.le_total estimate actual with hne | hne
    · -- estimate ≤ actual: the winner only gets cheaper, regret truncates to 0
      have sE := slope_split hpq estimate
      have sA := slope_split hpq actual
      have hle : (b.perRow - a.perRow) * estimate ≤ (b.perRow - a.perRow) * actual :=
        Nat.mul_le_mul_left _ hne
      rw [Nat.max_eq_right hpq, Nat.min_eq_left hpq, Nat.max_eq_right hne, Nat.min_eq_left hne]
      omega
    · -- actual ≤ estimate: full gap · (estimate − actual) may be lost
      have sE := slope_split hpq estimate
      have sA := slope_split hpq actual
      have hle : (b.perRow - a.perRow) * actual ≤ (b.perRow - a.perRow) * estimate :=
        Nat.mul_le_mul_left _ hne
      have mdist : (b.perRow - a.perRow) * (estimate - actual) =
          (b.perRow - a.perRow) * estimate - (b.perRow - a.perRow) * actual :=
        Nat.mul_sub_left_distrib _ _ _
      rw [Nat.max_eq_right hpq, Nat.min_eq_left hpq, Nat.max_eq_left hne, Nat.min_eq_right hne]
      omega
  · -- a.perRow is the steeper slope
    rcases Nat.le_total estimate actual with hne | hne
    · -- estimate ≤ actual: full gap · (actual − estimate) may be lost
      have sE := slope_split hpq estimate
      have sA := slope_split hpq actual
      have hle : (a.perRow - b.perRow) * estimate ≤ (a.perRow - b.perRow) * actual :=
        Nat.mul_le_mul_left _ hne
      have mdist : (a.perRow - b.perRow) * (actual - estimate) =
          (a.perRow - b.perRow) * actual - (a.perRow - b.perRow) * estimate :=
        Nat.mul_sub_left_distrib _ _ _
      rw [Nat.max_eq_left hpq, Nat.min_eq_right hpq, Nat.max_eq_right hne, Nat.min_eq_left hne]
      omega
    · -- actual ≤ estimate: the winner only gets cheaper, regret truncates to 0
      have sE := slope_split hpq estimate
      have sA := slope_split hpq actual
      have hle : (a.perRow - b.perRow) * actual ≤ (a.perRow - b.perRow) * estimate :=
        Nat.mul_le_mul_left _ hne
      rw [Nat.max_eq_left hpq, Nat.min_eq_right hpq, Nat.max_eq_left hne, Nat.min_eq_right hne]
      omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_CardinalityErrorRegret
