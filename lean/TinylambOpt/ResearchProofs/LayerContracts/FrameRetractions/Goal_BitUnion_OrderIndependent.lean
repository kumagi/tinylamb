import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `BitUnion.OrderIndependent` — Changing input order cannot change the output or the
  choice to rescan.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_OrderIndependent

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitUnion.OrderIndependent.Goal := by
  apply orderIndependent_of
  intro a b hp
  simpa [BitUnion.model] using perm_foldl_or hp

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_OrderIndependent
