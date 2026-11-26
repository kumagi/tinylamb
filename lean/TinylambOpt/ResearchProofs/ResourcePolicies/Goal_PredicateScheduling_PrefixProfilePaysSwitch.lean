import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.PrefixProfilePaysSwitch.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_PrefixProfilePaysSwitch
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.PrefixProfilePaysSwitch

theorem refute : ¬ Goal := by
  intro h
  let a : Test := ⟨fun r => r == 0, fun _ => 1⟩
  let b : Test := ⟨fun r => r == 1, fun _ => 1⟩
  let rs : List Nat := [0, 1]
  have hpfx : work [b, a] (rs.take 1) + 0 < work [a, b] (rs.take 1) := by native_decide
  have hbad := h a b rs 1 0 hpfx
  simp [work, a, b, rs] at hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_PrefixProfilePaysSwitch
