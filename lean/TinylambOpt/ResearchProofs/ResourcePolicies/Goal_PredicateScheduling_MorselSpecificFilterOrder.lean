import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.MorselSpecificFilterOrder.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_MorselSpecificFilterOrder
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.MorselSpecificFilterOrder

theorem sound : Goal := by
  intro ts as_ bs a b has hbs
  simp only [survivors, List.filter_append]
  congr 1
  · apply List.filter_congr
    intro r _
    exact congrArg (fun x => decide (x = true)) (has.all_eq (f := fun t => t.keep r))
  · apply List.filter_congr
    intro r _
    exact congrArg (fun x => decide (x = true)) (hbs.all_eq (f := fun t => t.keep r))
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_MorselSpecificFilterOrder
