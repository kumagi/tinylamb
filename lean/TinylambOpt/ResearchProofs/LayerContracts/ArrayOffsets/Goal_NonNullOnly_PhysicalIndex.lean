import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_PhysicalIndex

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : NonNullOnly.PhysicalIndex.Goal := by
  exact Common.physicalIndex_NonNullOnly

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_PhysicalIndex
