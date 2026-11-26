import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_Independent

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Cascade.Independent.Goal := by
  intro s a b c d h
  simp [Cascade.model] at h
  dsimp [sameBag, finish, run, step, Cascade.model]
  constructor
  · exact (List.erase_comm a c : (s.parents.erase a).erase c = (s.parents.erase c).erase a) ▸ List.Perm.refl _
  · change (((s.children.filter (fun x => x != some a)).filter (fun x => x != some c))).Perm
      (((s.children.filter (fun x => x != some c)).filter (fun x => x != some a)))
    rw [filter_comm]

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_Independent
