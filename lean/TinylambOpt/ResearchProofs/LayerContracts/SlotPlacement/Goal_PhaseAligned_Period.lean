import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `PhaseAligned.Period` — `∀ r i, model.snap r (i+r.stripe+1) = model.snap r
  i+r.stripe+1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_PhaseAligned_Period

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.PhaseAligned.Period.Goal := by
  exact PhaseAlignedFacts.period

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_PhaseAligned_Period
