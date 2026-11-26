import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt.ExactGenerationDrain.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_ExactGenerationDrain
open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt ExactGenerationDrain
theorem sound : Goal := by
  intro pending slot oldGen newGen hne
  induction pending with
  | nil => simp
  | cons t ts ih =>
    simp only [List.filter_cons, List.count_cons]
    by_cases ht : t = (slot, newGen)
    · subst ht
      have hbeq : ((slot, newGen) != (slot, oldGen)) = true := by
        simp [bne_iff_ne]; exact Ne.symm hne
      simp [hbeq, ih]
    · by_cases hto : (t != (slot, oldGen)) = true
      · simp [hto, ht, ih]
      · have hto' : (t != (slot, oldGen)) = false := by
          revert hto; cases (t != (slot, oldGen)) <;> simp
        simp [hto', ht, ih]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_ExactGenerationDrain
