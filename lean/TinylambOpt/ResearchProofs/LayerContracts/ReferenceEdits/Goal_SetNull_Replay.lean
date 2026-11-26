import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_Replay

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem refute : ¬ SetNull.Replay.Goal := by
  intro h
  have hh := h ⟨[0,0], []⟩ 0 0
  simpa [sameBag, finish, run, step, SetNull.model] using hh

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_Replay
