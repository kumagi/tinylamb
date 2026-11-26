import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_Independent

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : DropPair.Independent.Goal := by
  intro s a b c d h
  simp [DropPair.model] at h
  dsimp [sameBag, finish, run, step, DropPair.model]
  constructor
  · exact (List.erase_comm a c : (s.parents.erase a).erase c = (s.parents.erase c).erase a) ▸ List.Perm.refl _
  · change ((s.children.erase (some a)).erase (some c)).Perm
      ((s.children.erase (some c)).erase (some a))
    exact (List.erase_comm (some a) (some c) : (s.children.erase (some a)).erase (some c) = (s.children.erase (some c)).erase (some a)) ▸ List.Perm.refl _

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_Independent
