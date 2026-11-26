import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `ReserveHeader.ExactPage` — `∀ r, 0 < r.bytes → exhaustive model r (r.stripe+r.bytes)
  [] = some r.stripe`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ReserveHeader_ExactPage

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.ReserveHeader.ExactPage.Goal := by
    intro r hb
    apply find?_eq_some_of_sorted (range_pairwise_le (r.stripe + r.bytes))
    · simp
      omega
    · simp [fits, ReserveHeader.model, band, List.all_eq_true]
      exact hb
    · intro b hbmem hblt
      simp [fits, ReserveHeader.model, band, List.all_eq_true] at hbmem ⊢
      omega

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_ReserveHeader_ExactPage
