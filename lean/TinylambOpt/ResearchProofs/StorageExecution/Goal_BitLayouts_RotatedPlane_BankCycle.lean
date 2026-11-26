import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RotatedPlane.BankCycle` — `∀ n w i b, 0<n → layout.bit n w (i+n) b = layout.bit n w
  i b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_BankCycle

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane

/-- Advancing an ordinal by one full plane leaves the rotated bank unchanged. -/
theorem sound_BankCycle : RotatedPlane.BankCycle.Goal := by
  intro n w i b hn
  simp only [layout]
  rw [show i + n + b = (i + b) + n by omega, Nat.add_mod_right]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_BankCycle
