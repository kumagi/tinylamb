import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ParentFirst_Independent

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ParentFirst.Independent.Goal := by
  intro s a b c d h
  simp [sameBag, finish, run, step, ParentFirst.model]
  constructor
  · exact List.Perm.swap a c s.parents
  · exact List.Perm.swap (some b) (some d) s.children

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ParentFirst_Independent
