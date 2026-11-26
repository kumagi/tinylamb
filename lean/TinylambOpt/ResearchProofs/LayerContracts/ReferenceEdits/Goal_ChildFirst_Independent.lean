import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_Independent

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ChildFirst.Independent.Goal := by
  intro s a b c d h
  simp [ChildFirst.model] at h
  dsimp [sameBag, finish, run, step, ChildFirst.model]
  constructor
  · exact (List.Perm.cons d (List.Perm.swap b c (a :: s.parents))).trans
      ((List.Perm.swap b d (c :: a :: s.parents)).trans
        ((List.Perm.cons b (List.Perm.cons d (List.Perm.swap a c s.parents))).trans
          (List.Perm.cons b (List.Perm.swap a d (c :: s.parents)))))
  · exact List.Perm.swap (some a) (some c) s.children

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_Independent
