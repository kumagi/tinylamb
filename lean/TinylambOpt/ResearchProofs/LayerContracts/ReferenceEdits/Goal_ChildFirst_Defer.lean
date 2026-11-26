import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Refuted by counterexample

- `ChildFirst.Defer` — Can an immediate checker be replaced by an end-of-statement
  check?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_Defer

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem refute : ¬ ChildFirst.Defer.Goal := by
  intro h
  have hh := h {} 0 0 (by native_decide)
  revert hh
  native_decide

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_Defer
