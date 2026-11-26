import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `Contiguous.PayloadMonotone` — Shrinking payload size cannot invalidate an already
  viable address.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_PayloadMonotone

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.Contiguous.PayloadMonotone.Goal := by
  exact payloadMonotone_of Contiguous.model ContiguousFacts.fpSub ContiguousFacts.legalMono

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_PayloadMonotone
