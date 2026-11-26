import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ReplaceParent_SingleProbe

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ReplaceParent.SingleProbe.Goal := by
  intro s a b hs
  have hsProp := (ok_iff_prop s).mp hs
  have ht : ok (finish ReplaceParent.model s a b) = true := by
    rw [ok_iff_prop]
    intro k hk
    have hmem : some k ∈ s.children := by simpa [finish, run, step, ReplaceParent.model] using hk.1
    have hpar : k ∉ s.parents := by
      intro hp
      by_cases hka : k = a
      · subst k
        exact hk.2 (by simp [finish, run, step, ReplaceParent.model])
      · exact hk.2 (by simp [finish, run, step, ReplaceParent.model, hp, hka])
    exact hsProp k ⟨hmem, hpar⟩
  have hnot : ¬ bad (finish ReplaceParent.model s a b) a = true := by
    intro hb
    have hh := (bad_iff _ a).mp hb
    exact hh.2 (by simp [finish, run, step, ReplaceParent.model])
  have hba : bad (finish ReplaceParent.model s a b) a = false := by
    cases hb : bad (finish ReplaceParent.model s a b) a <;> simp_all
  have hiff : bad (finish ReplaceParent.model s a b) a = false ↔ ok (finish ReplaceParent.model s a b) = true :=
    ⟨fun _ => ht, fun _ => hba⟩
  exact singleProbe_of_head_iff ReplaceParent.model (fun _ _ => rfl) s a b hiff

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ReplaceParent_SingleProbe
