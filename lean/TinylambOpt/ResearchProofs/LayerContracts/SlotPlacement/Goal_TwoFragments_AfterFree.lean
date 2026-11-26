import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Refuted by counterexample

- `TwoFragments.AfterFree` — Removing an occupied byte may reveal an earlier first-fit
  address.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_AfterFree

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.TwoFragments.AfterFree.Goal := by
  intro h
  obtain ⟨j, hj, hle⟩ := h ⟨1, 0, 1⟩ 4 [0, 2] 0 1 (by native_decide)
  simp at hj
  have hq : quick TwoFragments.model ⟨1, 0, 1⟩ 4 [2] = none := by native_decide
  rw [hq] at hj
  contradiction

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_TwoFragments_AfterFree
