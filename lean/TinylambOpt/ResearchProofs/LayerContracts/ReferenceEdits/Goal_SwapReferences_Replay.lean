import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_Replay

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : SwapReferences.Replay.Goal := by
  intro s a b
  dsimp [finish, run, step, SwapReferences.model, replay]
  constructor
  · rfl
  · change ((((s.children.map (rwFun a (some b))).map (rwFun b (some a))).map (rwFun a (some b))).map (rwFun b (some a))).Perm
        (((s.children.map (rwFun a (some b))).map (rwFun b (some a))))
    rw [map_rwFun_swap_idem]

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_Replay
