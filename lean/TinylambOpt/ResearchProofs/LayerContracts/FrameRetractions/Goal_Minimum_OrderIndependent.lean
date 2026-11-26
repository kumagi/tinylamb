import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Minimum.OrderIndependent` — Changing input order cannot change the output or the
  choice to rescan.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_OrderIndependent

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Minimum.OrderIndependent.Goal := by
  apply orderIndependent_of
  intro a b hp
  simpa [Minimum.model] using perm_min hp

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_OrderIndependent
