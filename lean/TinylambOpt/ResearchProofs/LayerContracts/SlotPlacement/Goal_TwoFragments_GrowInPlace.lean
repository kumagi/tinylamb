import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_GrowInPlace

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.TwoFragments.GrowInPlace.Goal := by
  intro h
  have hf : fits TwoFragments.model ⟨1, 1, 1⟩ 4 [2] 1 = true := by native_decide
  obtain ⟨i, hi, hle⟩ := h ⟨1, 1, 1⟩ 4 [2] hf
  have hq : quick TwoFragments.model ⟨1, 1, 1⟩ 4 [2] = none := by native_decide
  rw [hq] at hi
  contradiction

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_GrowInPlace
