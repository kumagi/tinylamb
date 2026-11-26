import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `DyadicTile.TileDecode` — `∀ x y, (key (x,y)).2/16 = x%16 ∧ (key (x,y)).2%16 = y%16`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_TileDecode

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.DyadicTile

theorem sound_TileDecode : DyadicTile.TileDecode.Goal := by
  intro x y
  constructor
  · have h : y % 16 < 16 := Nat.mod_lt y (by decide)
    simp only [DyadicTile.key]
    omega
  · have h : y % 16 < 16 := Nat.mod_lt y (by decide)
    simp only [DyadicTile.key]
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_TileDecode
