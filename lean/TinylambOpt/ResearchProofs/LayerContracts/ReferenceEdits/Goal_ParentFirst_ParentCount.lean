import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `ParentFirst.ParentCount` — `∀ s a b, (finish model s a b).parents.count a =
  s.parents.count a+1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ParentFirst_ParentCount

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ParentFirst.ParentCount.Goal := by
  intro s a b
  simp [finish, run, step, ParentFirst.model]

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ParentFirst_ParentCount
