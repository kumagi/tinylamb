import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `Cascade.NewViolations` — Precision of the incremental violation report, even on
  already dirty input.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_NewViolations

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Cascade.NewViolations.Goal := by
  exact newViolations_of_frameKeys _ frameKeys_Cascade

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_NewViolations
