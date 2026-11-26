import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `MirrorZones.BoundaryTax` — `∀ z h a b, a≤b → price z h a b = price z 0 a b +
  h*(b/(z+1)-a/(z+1))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_BoundaryTax

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.MirrorZones

theorem sound_BoundaryTax : MirrorZones.BoundaryTax.Goal := by
  intro z h a b hab
  unfold price span
  simp
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_BoundaryTax
