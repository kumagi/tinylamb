import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Refuted by counterexample

- `TwoFragments.FirstFit` — Return the same lowest address as the dense allocator,
  including failure.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_FirstFit

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.TwoFragments.FirstFit.Goal := by
  intro h
  have hh := h ⟨1, 0, 1⟩ 4 [2]
  have hq : quick TwoFragments.model ⟨1, 0, 1⟩ 4 [2] = none := by native_decide
  have he : exhaustive TwoFragments.model ⟨1, 0, 1⟩ 4 [2] = some 1 := by native_decide
  rw [hq, he] at hh
  contradiction

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_FirstFit
