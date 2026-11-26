import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `DropPair.DeltaCheck` — A previously valid database needs revalidation only on the
  declared key set.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_DeltaCheck

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : DropPair.DeltaCheck.Goal := by
  exact deltaCheck_of_frameKeys _ frameKeys_DropPair

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_DeltaCheck
