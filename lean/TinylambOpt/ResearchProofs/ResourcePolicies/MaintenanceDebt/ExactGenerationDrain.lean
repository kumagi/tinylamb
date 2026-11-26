import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_MaintenanceDebt_ExactGenerationDrain
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
end Nov26.Proof_MaintenanceDebt_ExactGenerationDrain
