import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_NextBoundary

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.TwoFragments.NextBoundary.Goal := by
  apply nextBoundary_of TwoFragments.model
  · intro r i
    exact Nat.le_refl i
  · intro r i j hij hleg
    exact hij
  · intro r i hleg
    rfl
  · intro r i j hij hleg
    simp [TwoFragments.model]

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_NextBoundary
