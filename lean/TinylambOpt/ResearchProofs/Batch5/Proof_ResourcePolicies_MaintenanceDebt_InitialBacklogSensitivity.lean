/-
Scratch proof for `InitialBacklogSensitivity.Goal`. Starting with more debt
yields more ending debt, but the difference can never grow: a fixed future
load is a 1-Lipschitz function of the initial backlog. Thus a stopped writer's
residual debt cannot be amplified by replaying the same service trace.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/-!
Proof contribution for ResourcePolicies_MaintenanceDebt_InitialBacklogSensitivity. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MaintenanceDebt.Goal_InitialBacklogSensitivity

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt


/-- A single service step is monotone in the incoming debt. -/
theorem step_mono (t : Tick) {a b : Nat} (h : a ≤ b) : step a t ≤ step b t := by
  unfold step
  exact Nat.sub_le_sub_right (Nat.add_le_add_right h t.added) t.capacity

/-- A whole trace is monotone in the incoming debt. -/
theorem endDebt_mono (ts : List Tick) {a b : Nat} (h : a ≤ b) : endDebt a ts ≤ endDebt b ts := by
  induction ts generalizing a b with
  | nil => simpa [endDebt] using h
  | cons t ts ih =>
      change endDebt (step a t) ts ≤ endDebt (step b t) ts
      exact ih (step_mono t h)

/-- A single service step is 1-Lipschitz in the incoming debt. -/
theorem step_lipschitz (t : Tick) {a b : Nat} (h : a ≤ b) : step b t - step a t ≤ b - a := by
  unfold step
  omega

/-- Fixed future work is 1-Lipschitz in the initial backlog. -/
theorem sound : InitialBacklogSensitivity.Goal := by
  intro ts a b hab
  induction ts generalizing a b with
  | nil => simp [endDebt]
  | cons t ts ih =>
      change endDebt (step b t) ts - endDebt (step a t) ts ≤ b - a
      have hstep := step_mono t hab
      have hih := ih (step a t) (step b t) hstep
      exact Nat.le_trans hih (step_lipschitz t hab)

end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MaintenanceDebt.Goal_InitialBacklogSensitivity
