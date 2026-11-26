import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `SwapReferences.Independent` — Disjoint logical key footprints allow two statements
  to commute up to bag order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_Independent

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : SwapReferences.Independent.Goal := by
  intro s a b c d h
  simp [SwapReferences.model] at h
  dsimp [sameBag, finish, run, step, SwapReferences.model]
  constructor
  · rfl
  · change ((((s.children.map (rwFun a (some b))).map (rwFun b (some a))).map (rwFun c (some d))).map (rwFun d (some c))).Perm
      ((((s.children.map (rwFun c (some d))).map (rwFun d (some c))).map (rwFun a (some b))).map (rwFun b (some a)))
    rw [map_swapFun_comm _ a b c d h.1.1 h.1.2 h.2.1 h.2.2]

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_Independent
