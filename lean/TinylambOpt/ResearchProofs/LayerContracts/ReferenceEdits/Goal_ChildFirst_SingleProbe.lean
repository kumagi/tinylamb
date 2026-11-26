import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_SingleProbe

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ChildFirst.SingleProbe.Goal := by
  intro s a b hs
  have ht : ok (finish ChildFirst.model s a b) = true := by
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
  have hba : bad (finish ChildFirst.model s a b) a = false := (ok_iff_no_bad _).mp ht a
  have hiff : bad (finish ChildFirst.model s a b) a = false ↔ ok (finish ChildFirst.model s a b) = true :=
    ⟨fun _ => ht, fun _ => hba⟩
  exact singleProbe_of_head_iff ChildFirst.model (fun _ _ => rfl) s a b hiff

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_SingleProbe
