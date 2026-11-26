import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `Cascade.DroppedRows` — `∀ s a b, (finish model s a b).children.length +
  s.children.count (some a) = s.children.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_DroppedRows

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Cascade.DroppedRows.Goal := by
  intro s a b
  simpa [finish, run, step, Cascade.model] using length_filter_purge s.children a

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_DroppedRows
