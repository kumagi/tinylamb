import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `CompareSwap.ParallelFold` — Three records can be fused using either successful
  parenthesization.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_ParallelFold

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.CompareSwap.ParallelFold.Goal := by
  exact Common.compareSwap_parallelFold

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_ParallelFold
