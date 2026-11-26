import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_TouchEnd

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.Contiguous.TouchEnd.Goal := by
    intro r i hb
    induction hbytes : r.bytes with
    | zero => omega
    | succ n =>
        simp [Contiguous.model, band, List.getLast?_map, List.getLast?_range, hbytes]

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_TouchEnd
