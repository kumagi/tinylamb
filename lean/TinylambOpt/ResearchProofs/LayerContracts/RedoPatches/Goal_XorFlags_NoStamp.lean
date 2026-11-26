import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Refuted by counterexample

- `XorFlags.NoStamp` — Blindly repeating a patch after recovery is observationally
  idempotent.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_NoStamp

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.XorFlags.NoStamp.Goal := by
  exact Common.not_xorFlags_noStamp

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_NoStamp
