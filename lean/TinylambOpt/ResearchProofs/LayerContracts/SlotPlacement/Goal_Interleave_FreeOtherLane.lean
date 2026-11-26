import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Interleave_FreeOtherLane

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.Interleave.FreeOtherLane.Goal := by
  intro r cap u i hfit
  have h := hfit
  simp [fits, Interleave.model, List.all_eq_true] at h ⊢
  exact h

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Interleave_FreeOtherLane
