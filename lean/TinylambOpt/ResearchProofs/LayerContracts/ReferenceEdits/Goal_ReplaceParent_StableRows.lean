import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `ReplaceParent.StableRows` — Physical permutation of input rows preserves all final
  parent and child counts.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ReplaceParent_StableRows

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ReplaceParent.StableRows.Goal := by
  exact stableRows_of_model _

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ReplaceParent_StableRows
