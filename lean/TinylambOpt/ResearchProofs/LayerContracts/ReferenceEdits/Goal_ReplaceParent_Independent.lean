import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ReplaceParent_Independent

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ReplaceParent.Independent.Goal := by
  intro s a b c d h
  simp [ReplaceParent.model] at h
  dsimp [sameBag, finish, run, step, ReplaceParent.model]
  have hac : a ≠ c := h
  have hca : c ≠ a := Ne.symm hac
  simp [hac, hca]
  rw [List.erase_comm a c]
  exact List.Perm.swap a c _

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ReplaceParent_Independent
