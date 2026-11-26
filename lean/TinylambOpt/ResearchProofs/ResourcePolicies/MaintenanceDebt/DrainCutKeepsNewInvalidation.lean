import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt

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
