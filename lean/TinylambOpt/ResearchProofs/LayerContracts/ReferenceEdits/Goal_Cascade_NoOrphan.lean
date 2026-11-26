import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_NoOrphan

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Cascade.NoOrphan.Goal := by
  intro s a b hs
  let s1 := step s (.purge a)
  let s2 := step s1 (.delP a)
  have hs1 : ok s1 = true := by simpa [s1] using ok_purge s a hs
  have hno : ¬ some a ∈ s1.children := by simpa [s1] using no_child_after_purge s a
  have hs2 : ok s2 = true := by simpa [s2] using ok_delP_of_no_children s1 a hs1 hno
  have hc : ok s = true ∧ ok s1 = true ∧ ok s2 = true := ⟨hs, hs1, hs2⟩
  simpa [immediate, finish, run, step, Cascade.model, s1, s2] using hc

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_NoOrphan
