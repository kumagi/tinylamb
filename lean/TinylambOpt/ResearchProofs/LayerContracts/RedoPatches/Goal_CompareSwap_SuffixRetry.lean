import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `CompareSwap.SuffixRetry` — Replaying an already-applied sorted prefix before the
  suffix is harmless.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_SuffixRetry

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.CompareSwap.SuffixRetry.Goal := by
  exact Common.compareSwap_suffixRetry

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_SuffixRetry
