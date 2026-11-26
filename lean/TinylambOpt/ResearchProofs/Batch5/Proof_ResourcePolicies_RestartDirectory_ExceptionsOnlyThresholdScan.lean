import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_ExceptionsOnlyThresholdScan

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
A threshold at or above the inline lane maximum makes every matching value an
exception, so scanning only the exception stream covers the whole result.

The modeled scan keeps the same qualifying values; only the representation
from which they are read changes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_ExceptionsOnlyThresholdScan

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

theorem filter_filter_of_imp (p q : Nat → Bool) (himp : ∀ x, q x = true → p x = true)
    (xs : List Nat) : (xs.filter p).filter q = xs.filter q := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      by_cases hq : q x
      · by_cases hp : p x
        · simp [hp, hq, ih]
        · exfalso
          exact hp (himp x hq)
      · by_cases hp : p x
        · simp [hp, hq, ih]
        · simp [hp, hq, ih]

theorem sound : ExceptionsOnlyThresholdScan.Goal := by
  intro w xs threshold ht
  apply filter_filter_of_imp
  intro x hx
  unfold flagged
  simp only [decide_eq_true_eq] at hx ⊢
  exact Nat.le_trans ht hx

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_ExceptionsOnlyThresholdScan
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_ExceptionsOnlyThresholdScan
