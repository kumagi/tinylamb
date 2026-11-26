import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `Adopt.Independent` — Disjoint logical key footprints allow two statements to commute
  up to bag order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Adopt_Independent

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Adopt.Independent.Goal := by
  intro s a b c d h
  simp [Adopt.model] at h
  dsimp [sameBag, finish, run, step, Adopt.model]
  constructor
  · exact parents_add_del_comm s.parents a b c d h.1.1 h.1.2 h.2.1 h.2.2
  · change ((s.children.map (rwFun a (some b))).map (rwFun c (some d))).Perm
      ((s.children.map (rwFun c (some d))).map (rwFun a (some b)))
    rw [map_rwFun_comm _ a c (some b) (some d) h.1.1 (by simp [h.2.1]) (by simpa using (Ne.symm h.1.2))]

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Adopt_Independent
