import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_PushBoth

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair

theorem sound_PushBoth : CappedPair.PushBoth.Goal := by
  intro t k x y hkt
  show k ≤ min t (min x y) ↔ k ≤ x ∧ k ≤ y
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_PushBoth
