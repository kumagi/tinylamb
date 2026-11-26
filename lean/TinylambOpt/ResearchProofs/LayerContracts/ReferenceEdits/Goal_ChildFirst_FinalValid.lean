import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `ChildFirst.FinalValid` — `∀ s a b, ok s → ok (finish model s a b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_FinalValid

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ChildFirst.FinalValid.Goal := by
  intro s a b hs
  rw [ok_iff_prop] at hs ⊢
  intro k hk
  have hmem : some k ∈ some a :: s.children := by simpa [finish, run, step, ChildFirst.model] using hk.1
  rcases List.mem_cons.mp hmem with hhead | htail
  · have hka : k = a := by injection hhead
    subst k
    exact hk.2 (by simp [finish, run, step, ChildFirst.model])
  · have hpar : k ∉ s.parents := by
      intro hp
      exact hk.2 (by simp [finish, run, step, ChildFirst.model, hp])
    exact hs k ⟨htail, hpar⟩

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_FinalValid
