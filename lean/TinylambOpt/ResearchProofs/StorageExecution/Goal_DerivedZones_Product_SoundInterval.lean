import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Product.SoundInterval` — Bound a computed group or predicate key from independent
  column min/max.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_SoundInterval

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

/-- Multiplication of naturals is monotone in both arguments, so the
componentwise min/max rectangle bounds the product. -/
theorem sound_SoundInterval : Product.SoundInterval.Goal := by
  intro t a b x y hx hy
  simp only [inside] at hx hy ⊢
  dsimp [kernel]
  exact ⟨Nat.mul_le_mul hx.1 hy.1, Nat.mul_le_mul hx.2 hy.2⟩

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_SoundInterval
