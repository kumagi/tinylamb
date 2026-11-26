import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `FenceStripe.PayloadMonotone` — Shrinking payload size cannot invalidate an already
  viable address.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_PayloadMonotone

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.FenceStripe.PayloadMonotone.Goal := by
  exact payloadMonotone_of FenceStripe.model (by
    intro r n i hn x hx
    apply band_subset hn x hx) (by
    intro r n i hn hleg
    simp [FenceStripe.model] at hleg ⊢
    omega)

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_PayloadMonotone
