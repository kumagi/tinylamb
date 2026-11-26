import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `SlotGroup.Waste` — `∀ r i, (model.footprint r i).length-r.bytes < 4`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_SlotGroup_Waste

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.SlotGroup.Waste.Goal := by
    intro r i
    have hge := align_ge r.bytes 0 4 (by omega)
    have hle_div : r.bytes + 4 - 1 ≤ (r.bytes + 4 - 1) / 4 * 4 + 4 - 1 :=
      (Nat.div_le_iff_le_mul (by omega : 0 < 4)).1 (Nat.le_refl _)
    simp [SlotGroup.model, band]
    change align r.bytes 0 4 - r.bytes < 4
    unfold align
    omega

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_SlotGroup_Waste
