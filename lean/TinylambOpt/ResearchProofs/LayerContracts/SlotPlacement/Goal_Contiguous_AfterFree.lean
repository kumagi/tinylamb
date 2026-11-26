import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `Contiguous.AfterFree` — Removing an occupied byte may reveal an earlier first-fit
  address.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_AfterFree

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.Contiguous.AfterFree.Goal := by
  apply afterFree_of_firstFit Contiguous.model
  apply firstFit_of Contiguous.model ContiguousFacts.prev
  · exact ContiguousFacts.fix
  · exact ContiguousFacts.prevSpec

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_AfterFree
