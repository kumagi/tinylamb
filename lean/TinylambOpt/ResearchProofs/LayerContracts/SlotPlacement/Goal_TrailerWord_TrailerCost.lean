import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `TrailerWord.TrailerCost` — `∀ r i, (model.footprint r i).length = r.bytes+4`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TrailerWord_TrailerCost

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.TrailerWord.TrailerCost.Goal := by
    intro r i
    simp [TrailerWord.model, band]

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TrailerWord_TrailerCost
