import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_NewNulls

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : SetNull.NewNulls.Goal := by
  intro s a b
  simpa [finish, run, step, SetNull.model] using count_none_map_null_lit s.children a

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_NewNulls
