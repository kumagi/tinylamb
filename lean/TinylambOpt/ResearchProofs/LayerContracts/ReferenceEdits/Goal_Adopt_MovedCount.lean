import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Adopt_MovedCount

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Adopt.MovedCount.Goal := by
  intro s a b hab
  simpa [finish, run, step, Adopt.model] using count_some_map_rewrite_tgt_lit s.children a b hab

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Adopt_MovedCount
