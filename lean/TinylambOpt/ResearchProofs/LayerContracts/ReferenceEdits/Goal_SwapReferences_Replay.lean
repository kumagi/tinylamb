import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `SwapReferences.Replay` — Blind retry of the statement after lost acknowledgement
  preserves both bags.
-/

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
