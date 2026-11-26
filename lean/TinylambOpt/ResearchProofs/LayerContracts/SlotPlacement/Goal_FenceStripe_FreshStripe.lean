import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `FenceStripe.FreshStripe` — `∀ r i, r.bytes ≤ r.stripe+1 → model.legal r
  (i*(r.stripe+1))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_FreshStripe

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.FenceStripe.FreshStripe.Goal := by
    intro r i hb
    simp [FenceStripe.model]
    have hmod : (i * (r.stripe+1)) % (r.stripe+1) = 0 := by
      rw [Nat.mul_comm i (r.stripe+1), Nat.mul_mod_right]
    omega

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_FreshStripe
