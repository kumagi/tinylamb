import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `TwoFragments.HoleFree` — `∀ r i x, i < x → x ≤ i+r.stripe → x ∉ model.footprint r i`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_HoleFree

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.TwoFragments.HoleFree.Goal := by
    intro r i x hix hxi
    simp [TwoFragments.model, band]
    constructor
    · omega
    · intro k hk
      omega

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_HoleFree
