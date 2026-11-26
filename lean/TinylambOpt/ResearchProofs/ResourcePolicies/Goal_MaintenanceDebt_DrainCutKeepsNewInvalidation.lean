import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt.DrainCutKeepsNewInvalidation.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_DrainCutKeepsNewInvalidation

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

theorem drainCutKeepsNewInvalidation : DrainCutKeepsNewInvalidation.Goal := by
  intro old fresh k n hnodup
  simp only [queue, List.mem_eraseDups, List.mem_append]
  constructor
  · intro h
    rcases h with hd | hf
    · by_cases ht : k ∈ old.take n
      · have hnodup' : (old.take n ++ old.drop n).Nodup := by
          rwa [List.take_append_drop n old]
        rcases List.nodup_append.mp hnodup' with ⟨_, _, hdisj⟩
        exact (hdisj k ht k hd rfl).elim
      · exact Or.inl ⟨by
          have : k ∈ old.take n ++ old.drop n := List.mem_append.mpr (Or.inr hd)
          rwa [List.take_append_drop n old] at this, ht⟩
    · exact Or.inr hf
  · intro h
    rcases h with ⟨hkold, hkn⟩ | hfresh
    · have hsplit : k ∈ old.take n ++ old.drop n := by
        rw [List.take_append_drop n old]; exact hkold
      rcases List.mem_append.mp hsplit with ht | hd
      · exact absurd ht hkn
      · exact Or.inl hd
    · exact Or.inr hfresh
