import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Refuted by counterexample

- `Consume.NewestOnly` — A last-record-per-cell index reconstructs both value and
  stamp.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Consume_NewestOnly

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Consume.NewestOnly.Goal := by
  exact Common.not_consume_newestOnly

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Consume_NewestOnly
