import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.CanceledGrantByteConservation.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_CanceledGrantByteConservation

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

private theorem sum_map_filter_add_sum_map_filter_not (f : Ticket → Nat) (p : Ticket → Bool) (ts : List Ticket) :
    ((ts.filter p).map f).sum + ((ts.filter (fun t => !p t)).map f).sum = (ts.map f).sum := by
  induction ts with
  | nil => simp
  | cons t ts ih =>
      by_cases hp : p t = true
      · simp [hp, ih]; omega
      · have hp' : p t = false := by cases h : p t <;> simp_all
        simp [hp', ih]; omega

theorem canceledGrantByteConservation : CanceledGrantByteConservation.Goal := by
  intro ts owner gen
  unfold liveBytes revoke
  dsimp
  let p : Ticket → Bool := fun t => !(t.owner == owner && t.generation == gen)
  have hdouble : (ts.filter p).filter p = ts.filter p := by
    rw [List.filter_filter]
    congr 1; funext t; cases h : p t <;> simp [h]
  have hpart := sum_map_filter_add_sum_map_filter_not Ticket.bytes p ts
  have hnotp : (ts.filter (fun t => t.owner == owner && t.generation == gen)) = ts.filter (fun t => !p t) := by
    congr 1; funext t; simp [p]
  -- rewrite goal using these facts
  change (List.map Ticket.bytes ts).sum - (List.map Ticket.bytes (List.filter p ts)).sum +
      ((List.map Ticket.bytes (List.filter p ts)).sum -
        (List.map Ticket.bytes ((List.filter p ts).filter p)).sum) =
    (List.map Ticket.bytes (List.filter (fun t => t.owner == owner && t.generation == gen) ts)).sum
  rw [hdouble, hnotp]
  omega
