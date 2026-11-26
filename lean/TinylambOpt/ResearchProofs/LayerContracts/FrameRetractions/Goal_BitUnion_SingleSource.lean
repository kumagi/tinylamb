import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_SingleSource

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

theorem sound : BitUnion.SingleSource.Goal := by
  intro x hx
  have hxne : x ≠ 0 := by omega
  simp [repaired, dropOne, value, BitUnion.model, hxne]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_SingleSource
