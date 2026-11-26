import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `RatioClass.Reconstruction` — `∀ x y, (key (x,y)).1*(y+1)+(key (x,y)).2=x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_Reconstruction

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.RatioClass

/-! The quotient/remainder pair is the division algorithm: for every `x` and
    `y`, `x = (x / (y+1)) * (y+1) + x % (y+1)`.  The divisor `y+1` is positive
    because `y : Nat`, so `Nat.div_add_mod` applies unconditionally. -/

theorem sound_Reconstruction : RatioClass.Reconstruction.Goal := by
  intro x y
  change (x / (y + 1)) * (y + 1) + x % (y + 1) = x
  simpa [Nat.mul_comm] using Nat.div_add_mod x (y + 1)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_Reconstruction
