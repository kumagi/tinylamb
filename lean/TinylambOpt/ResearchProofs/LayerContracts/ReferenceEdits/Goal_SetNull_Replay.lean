import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Refuted by counterexample

- `SetNull.Replay` — Blind retry of the statement after lost acknowledgement preserves
  both bags.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_Replay

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem refute : ¬ SetNull.Replay.Goal := by
  intro h
  have hh := h ⟨[0,0], []⟩ 0 0
  simpa [sameBag, finish, run, step, SetNull.model] using hh

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SetNull_Replay
