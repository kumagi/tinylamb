import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ReserveHeader_HeaderFloor

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.ReserveHeader.HeaderFloor.Goal := by
    intro r cap u i hq
    have hf : fits ReserveHeader.model r cap u i = true := List.find?_some (by simpa [quick] using hq)
    simp [fits, ReserveHeader.model] at hf
    exact hf.1.2

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ReserveHeader_HeaderFloor
