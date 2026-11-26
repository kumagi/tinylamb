import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `Interleave.Parity` — `∀ r i x, x ∈ model.footprint r i → x%2 = i%2`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Interleave_Parity

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.Interleave.Parity.Goal := by
    intro r i x hx
    simp [Interleave.model] at hx
    rcases hx with ⟨k, hk, rfl⟩
    omega

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Interleave_Parity
