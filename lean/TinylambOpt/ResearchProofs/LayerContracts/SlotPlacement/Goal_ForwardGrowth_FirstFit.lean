import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `ForwardGrowth.FirstFit` — Return the same lowest address as the dense allocator,
  including failure.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ForwardGrowth_FirstFit

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.ForwardGrowth.FirstFit.Goal := by
  apply firstFit_of ForwardGrowth.model ForwardGrowthFacts.prev
  · exact ForwardGrowthFacts.fix
  · exact ForwardGrowthFacts.prevSpec

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ForwardGrowth_FirstFit
