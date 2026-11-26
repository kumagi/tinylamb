import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
`bill` is affine in the row count, so the difference of the two bills changes monotonically
in `n`: with slope `a.perRow - b.perRow`. If `a.perRow ≤ b.perRow` the advantage of `a` can
only grow as `n` increases, so dominance at the left endpoint controls every `n ≥ lo`; if
`b.perRow ≤ a.perRow` dominance at the right endpoint controls every `n ≤ hi`. Either way
one memo entry serves the whole interval. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_IntervalPlanStability
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

private theorem le_of_succ_le (n m : Nat) (h : n + 1 ≤ m) : n ≤ m := by omega

/-- If `a`'s bill is ahead at `x` and `a`'s slope is no steeper, it stays ahead at `x + 1`. -/
private theorem step_up (pa pb s1 s2 x : Nat) (hab : pa ≤ pb)
    (h : s1 + pa * x ≤ s2 + pb * x) : s1 + pa * (x + 1) ≤ s2 + pb * (x + 1) := by
  have e1 : pa * (x + 1) = pa * x + pa := Nat.mul_succ pa x
  have e2 : pb * (x + 1) = pb * x + pb := Nat.mul_succ pb x
  omega

/-- If `a`'s bill is ahead at `y + 1` and `a`'s slope is no shallower, it was ahead at `y`. -/
private theorem step_down (pa pb s1 s2 y : Nat) (hba : pb ≤ pa)
    (h : s1 + pa * (y + 1) ≤ s2 + pb * (y + 1)) : s1 + pa * y ≤ s2 + pb * y := by
  have e1 : pa * (y + 1) = pa * y + pa := Nat.mul_succ pa y
  have e2 : pb * (y + 1) = pb * y + pb := Nat.mul_succ pb y
  omega

theorem sound : IntervalPlanStability.Goal := by
  intro a b lo hi n hlo hhh hle hhi
  simp only [bill] at hle hhi ⊢
  by_cases hab : a.perRow ≤ b.perRow
  · have claim : ∀ d : Nat, a.setup + a.perRow * (lo + d) ≤ b.setup + b.perRow * (lo + d) := by
      intro d
      induction d with
      | zero => simpa using hle
      | succ d ih => exact step_up a.perRow b.perRow a.setup b.setup (lo + d) hab ih
    have h2 := claim (n - lo)
    rw [Nat.add_sub_cancel' hlo] at h2
    exact h2
  · have hba : b.perRow ≤ a.perRow := by omega
    have claim : ∀ d : Nat, d ≤ hi →
        a.setup + a.perRow * (hi - d) ≤ b.setup + b.perRow * (hi - d) := by
      intro d
      induction d with
      | zero => intro _; simpa using hhi
      | succ d ih =>
          intro hd
          have hy : hi - d.succ + 1 = hi - d := by omega
          have := ih (by omega)
          rw [← hy] at this
          exact step_down a.perRow b.perRow a.setup b.setup (hi - d.succ) hba this
    have key : hi - (hi - n) = n := by omega
    rw [← key]
    exact claim (hi - n) (by omega)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_IntervalPlanStability
