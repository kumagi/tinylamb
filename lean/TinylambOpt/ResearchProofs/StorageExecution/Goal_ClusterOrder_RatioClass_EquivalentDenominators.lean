import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `RatioClass.EquivalentDenominators` — `∀ x y z, x≤y → x≤z → key (x,y)=key (x,z)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_EquivalentDenominators

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.RatioClass

theorem sound_EquivalentDenominators : RatioClass.EquivalentDenominators.Goal := by
  intro x y z hxy hxz
  have hy : x < y + 1 := by omega
  have hz : x < z + 1 := by omega
  simp [RatioClass.key, Nat.div_eq_of_lt hy, Nat.mod_eq_of_lt hy,
    Nat.div_eq_of_lt hz, Nat.mod_eq_of_lt hz]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_EquivalentDenominators
