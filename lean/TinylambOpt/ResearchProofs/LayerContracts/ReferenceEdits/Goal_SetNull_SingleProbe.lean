import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `SetNull.SingleProbe` — Can checking just the first affected key authorize the
  complete statement?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_SingleProbe

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : SetNull.SingleProbe.Goal := by
  intro s a b hs
  let s1 := step s (.rewrite a none)
  let s2 := step s1 (.delP a)
  have hs1 : ok s1 = true := by simpa [s1] using ok_rewrite_none s a hs
  have hno : ¬ some a ∈ s1.children := by simpa [s1] using no_child_after_rewrite_none s a
  have hs2 : ok s2 = true := by simpa [s2] using ok_delP_of_no_children s1 a hs1 hno
  have ht : ok (finish SetNull.model s a b) = true := by simpa [finish, run, step, SetNull.model, s1, s2] using hs2
  have hba : bad (finish SetNull.model s a b) a = false := (ok_iff_no_bad _).mp ht a
  have hiff : bad (finish SetNull.model s a b) a = false ↔ ok (finish SetNull.model s a b) = true :=
    ⟨fun _ => ht, fun _ => hba⟩
  exact singleProbe_of_head_iff SetNull.model (fun _ _ => rfl) s a b hiff

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_SingleProbe
