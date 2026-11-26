import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_AlignedWord_CapacityOnly

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.AlignedWord.CapacityOnly.Goal := by
  intro h
  have hh := h ⟨1, 0, 0⟩ 2 [0] (by decide) (by decide)
  have hv : (quick AlignedWord.model ⟨1, 0, 0⟩ 2 [0]).isSome = false := by native_decide
  rw [hv] at hh
  contradiction

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_AlignedWord_CapacityOnly
