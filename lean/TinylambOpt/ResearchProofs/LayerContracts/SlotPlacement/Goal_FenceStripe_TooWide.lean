import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `FenceStripe.TooWide` — `∀ r cap u, r.stripe+1 < r.bytes → exhaustive model r cap u =
  none`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_TooWide

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.FenceStripe.TooWide.Goal := by
    intro r cap u hw
    apply (List.find?_eq_none).2
    intro i hi
    simp [fits, FenceStripe.model]
    omega

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_TooWide
