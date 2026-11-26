import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_AlignedWord_Padding

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.AlignedWord.Padding.Goal := by
    intro r i
    change align i 0 8 - i < 8
    have hge := align_ge i 0 8 (by omega)
    have hle_div : i + 8 - 1 ≤ (i + 8 - 1) / 8 * 8 + 8 - 1 :=
      (Nat.div_le_iff_le_mul (by omega : 0 < 8)).1 (Nat.le_refl _)
    unfold align
    omega

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_AlignedWord_Padding
