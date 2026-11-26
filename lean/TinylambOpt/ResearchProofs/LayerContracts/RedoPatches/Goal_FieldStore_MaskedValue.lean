import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_MaskedValue

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.FieldStore.MaskedValue.Goal := by
  exact Common.fieldStore_maskedValue

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_MaskedValue
