import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `Adopt.MovedCount` — `∀ s a b, a ≠ b → (finish model s a b).children.count (some b) =
  s.children.count (some a)+s.children.count (some b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Adopt_MovedCount

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Adopt.MovedCount.Goal := by
  intro s a b hab
  simpa [finish, run, step, Adopt.model] using count_some_map_rewrite_tgt_lit s.children a b hab

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Adopt_MovedCount
