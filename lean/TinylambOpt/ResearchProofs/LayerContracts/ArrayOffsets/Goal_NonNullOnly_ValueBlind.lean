import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_ValueBlind

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem refute : ¬ NonNullOnly.ValueBlind.Goal := by
  exact Common.valueblind_NonNullOnly

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_ValueBlind
