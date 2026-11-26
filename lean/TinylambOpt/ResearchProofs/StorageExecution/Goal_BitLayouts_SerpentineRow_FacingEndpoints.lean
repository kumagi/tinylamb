import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `SerpentineRow.FacingEndpoints` — `∀ n w i, i%2=0 → 0<w → layout.bit n w i (w-1)+1 =
  layout.bit n w (i+1) (w-1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_FacingEndpoints

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow

theorem sound_FacingEndpoints : SerpentineRow.FacingEndpoints.Goal := by
  intro n w i hi hw
  dsimp [layout]
  simp only [hi, ↓reduceIte]
  have hi1 : (i + 1) % 2 ≠ 0 := by omega
  simp only [hi1, ↓reduceIte]
  have hw1 : w - 1 + 1 = w := by omega
  rw [Nat.add_assoc, hw1, Nat.succ_mul]
  simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_FacingEndpoints
