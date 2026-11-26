import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_ExactFailure

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.TwoFragments.ExactFailure.Goal := by
  intro h
  have hq : quick TwoFragments.model ⟨1, 0, 1⟩ 4 [2] = none := by native_decide
  have hh := (h ⟨1, 0, 1⟩ 4 [2]).1 hq
  have hf : fits TwoFragments.model ⟨1, 0, 1⟩ 4 [2] 1 = true := by native_decide
  have hh1 := hh 1
  simp [hf] at hh1

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_ExactFailure
