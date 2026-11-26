import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `Contiguous.TouchEnd` — `∀ r i, 0 < r.bytes → (model.footprint r i).getLast? = some
  (i+r.bytes-1)`
-/

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
