import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `TrailerWord.EmptyTag` — `∀ r, r.bytes = 0 → exhaustive model r 3 [] = none`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TrailerWord_EmptyTag

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.TrailerWord.EmptyTag.Goal := by
    intro r hr
    apply (List.find?_eq_none).2
    intro i hi
    have hicap : i < 3 := by simpa using hi
    by_cases hf : fits TrailerWord.model r 3 [] i = true
    · have h' := hf
      simp [fits, TrailerWord.model, band, List.all_eq_true, hr] at h'
      have hx : 3 - i < 4 := by omega
      have hbad := h'.2 (3-i) hx
      omega
    · simp [hf]

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TrailerWord_EmptyTag
