import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `SetNull.Independent` — Disjoint logical key footprints allow two statements to
  commute up to bag order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_Independent

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : SetNull.Independent.Goal := by
  intro s a b c d h
  simp [SetNull.model] at h
  dsimp [sameBag, finish, run, step, SetNull.model]
  constructor
  · exact (List.erase_comm a c : (s.parents.erase a).erase c = (s.parents.erase c).erase a) ▸ List.Perm.refl _
  · change ((s.children.map (rwFun a none)).map (rwFun c none)).Perm
      ((s.children.map (rwFun c none)).map (rwFun a none))
    rw [map_rwFun_comm _ a c none none h (by simp) (by simp)]

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_Independent
