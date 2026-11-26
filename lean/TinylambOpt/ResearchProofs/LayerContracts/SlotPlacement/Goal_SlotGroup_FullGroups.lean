import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `SlotGroup.FullGroups` — `∀ r i, (model.footprint r i).length % 4 = 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_SlotGroup_FullGroups

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.SlotGroup.FullGroups.Goal := by
    intro r i
    simp [SlotGroup.model, band]
    change (align r.bytes 0 4) % 4 = 0
    exact align_mod r.bytes 0 4 (by omega) (by omega)

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_SlotGroup_FullGroups
