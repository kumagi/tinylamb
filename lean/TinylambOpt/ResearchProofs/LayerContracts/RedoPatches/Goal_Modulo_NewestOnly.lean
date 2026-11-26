import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Refuted by counterexample

- `Modulo.NewestOnly` — A last-record-per-cell index reconstructs both value and stamp.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_NewestOnly

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Modulo.NewestOnly.Goal := by
  exact Common.not_modulo_newestOnly

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_NewestOnly
