import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Refuted by counterexample

- `FenceStripe.CapacityOnly` — Strong admission hypothesis: enough unoccupied bytes
  always allow this request.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_CapacityOnly

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.FenceStripe.CapacityOnly.Goal := by
  intro h
  have hh := h ⟨2, 0, 0⟩ 2 [] (by decide) (by decide)
  have hv : (quick FenceStripe.model ⟨2, 0, 0⟩ 2 []).isSome = false := by native_decide
  rw [hv] at hh
  contradiction

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_CapacityOnly
