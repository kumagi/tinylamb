import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ParentFirst_NeedsTarget

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ParentFirst.NeedsTarget.Goal := by
  intro s a b hs
  rw [ok_iff_prop] at hs ⊢
  constructor
  · intro hok
    by_cases hEq : a = b
    · exact Or.inl hEq
    · by_cases hmem : b ∈ s.parents
      · exact Or.inr hmem
      · exfalso
        exact hok b ⟨by simp [finish, run, step, ParentFirst.model],
          by simp [finish, run, step, ParentFirst.model, hEq, Ne.symm hEq, hmem]⟩
  · intro h k hk
    have hmem : some k ∈ some b :: s.children := by simpa [finish, run, step, ParentFirst.model] using hk.1
    rcases List.mem_cons.mp hmem with hhead | htail
    · have hkb : k = b := by injection hhead
      subst k
      rcases h with hEq | hmempar
      · subst b
        exact hk.2 (by simp [finish, run, step, ParentFirst.model])
      · exact hk.2 (by simp [finish, run, step, ParentFirst.model, hmempar])
    · have hpar : k ∉ s.parents := by
        intro hp
        exact hk.2 (by simp [finish, run, step, ParentFirst.model, hp])
      exact hs k ⟨htail, hpar⟩

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ParentFirst_NeedsTarget
