import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `ReserveHeader.NextBoundary` — From a cursor, skip to the rounded base. Every
  feasible placement lies at or beyond it; if it fits, a restricted dense search must
  return it first.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ReserveHeader_NextBoundary

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.ReserveHeader.NextBoundary.Goal := by
  exact nextBoundary_of ReserveHeader.model ReserveHeaderFacts.up ReserveHeaderFacts.before ReserveHeaderFacts.fix ReserveHeaderFacts.snapLegal

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ReserveHeader_NextBoundary
