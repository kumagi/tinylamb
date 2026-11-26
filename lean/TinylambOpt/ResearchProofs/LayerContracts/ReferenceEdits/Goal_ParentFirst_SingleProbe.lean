import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Refuted by counterexample

- `ParentFirst.SingleProbe` — Can checking just the first affected key authorize the
  complete statement?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ParentFirst_SingleProbe

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem refute : ¬ ParentFirst.SingleProbe.Goal := by
  intro h
  have hh := h {} 0 1 (by native_decide)
  revert hh
  native_decide

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ParentFirst_SingleProbe
