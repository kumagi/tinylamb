import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `TwoFragments.PayloadMonotone` — Shrinking payload size cannot invalidate an already
  viable address.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_PayloadMonotone

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.TwoFragments.PayloadMonotone.Goal := by
  exact payloadMonotone_of TwoFragments.model (by
    intro r n i hn x hx
    simp [TwoFragments.model, band] at hx ⊢
    rcases hx with rfl | ⟨k, hk, rfl⟩
    · simp
    · right
      exact ⟨k, by omega, rfl⟩) (by
    intro r n i hn hleg
    simp [TwoFragments.model])

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_PayloadMonotone
