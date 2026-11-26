import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `Reparent.KeepNulls` — `∀ s a b, (finish model s a b).children.count none =
  s.children.count none`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Reparent_KeepNulls

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Reparent.KeepNulls.Goal := by
  intro s a b
  simpa [finish, run, step, Reparent.model] using count_none_map_rewrite_lit s.children a b

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Reparent_KeepNulls
