import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.AvoidedEvaluationInterchange.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_AvoidedEvaluationInterchange
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.AvoidedEvaluationInterchange

private theorem sum_map_filter_add_not (f : Nat → Nat) (p : Nat → Bool) (l : List Nat) :
    ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    cases hp : p x <;> simp [List.filter, hp, ih, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]

theorem sound : Goal := by
  intro a b rest rs
  have hff : (rs.filter a.keep).filter b.keep = (rs.filter b.keep).filter a.keep := by
    rw [List.filter_filter, List.filter_filter]
    congr 1; funext r; simp [Bool.and_comm]
  simp only [work, saved]
  rw [hff]
  have ha := sum_map_filter_add_not a.cost b.keep rs
  have hb := sum_map_filter_add_not b.cost a.keep rs
  constructor <;> intro h <;> omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_AvoidedEvaluationInterchange
