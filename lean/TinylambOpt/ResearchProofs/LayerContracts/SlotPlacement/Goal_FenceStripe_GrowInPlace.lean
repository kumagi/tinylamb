import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_FenceStripe

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `FenceStripe.GrowInPlace` — If reusing the original base is legal, a prefix-limited
  search can find a fit.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_GrowInPlace

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts.FenceStripeFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.FenceStripe.GrowInPlace.Goal := by
  apply growInPlace_of_firstFit FenceStripe.model
  apply firstFit_of FenceStripe.model prev
  · exact fix
  · exact prevSpec

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_GrowInPlace
