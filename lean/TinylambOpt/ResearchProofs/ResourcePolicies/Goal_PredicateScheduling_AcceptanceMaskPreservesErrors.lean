import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.AcceptanceMaskPreservesErrors.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_AcceptanceMaskPreservesErrors
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.AcceptanceMaskPreservesErrors

theorem refute : ¬ Goal := by
  intro h
  let a : Nat → Outcome := fun r => if r == 0 then .fault 1 else .reject
  let b : Nat → Outcome := fun r => if r == 0 then .fault 2 else .reject
  have hmask : ([0,1] : List Nat).filter (fun r => evaluate [a,b] r == .pass) =
               ([0,1] : List Nat).filter (fun r => evaluate [b,a] r == .pass) := by native_decide
  have hbad := h a b [0,1] hmask
  simp [evaluate, a, b] at hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_AcceptanceMaskPreservesErrors
