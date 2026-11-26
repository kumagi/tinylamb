import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Product.ProductDivisor` — `∀ x y k, 0<x → (k≤kernel.score 0 (x,y) ↔ (k+x-1)/x≤y)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_ProductDivisor

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

theorem sound_ProductDivisor : Product.ProductDivisor.Goal := by
  intro x y k hx
  simp only [kernel]
  rw [Nat.div_le_iff_le_mul hx]
  have hx1 : 1 ≤ x := hx
  rw [Nat.add_sub_assoc hx1 k, Nat.add_sub_assoc hx1 (y * x)]
  constructor
  · intro h
    exact Nat.add_le_add_right (by simpa [Nat.mul_comm] using h) (x - 1)
  · intro h
    have h' : k ≤ y * x := Nat.add_le_add_iff_right.mp h
    simpa [Nat.mul_comm] using h'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_ProductDivisor
