import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.GlobalSelectivityOrderTransfer.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_GlobalSelectivityOrderTransfer
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.GlobalSelectivityOrderTransfer

theorem refute : ¬ Goal := by
  intro h
  let a : Test := ⟨fun r => r == 0 || r == 1, fun _ => 1⟩
  let b : Test := ⟨fun r => r == 0 || r == 2, fun _ => 1⟩
  let rs : List Nat := [0, 1, 2]
  let guard : Nat → Bool := fun r => r != 2
  have hglob : work [a, b] rs ≤ work [b, a] rs := by native_decide
  have hbad := h a b rs guard hglob
  simp [work, a, b, rs, guard] at hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_GlobalSelectivityOrderTransfer
