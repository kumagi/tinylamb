import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_UnorderedPair_OrientationTies

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.UnorderedPair

-- Truth check on small inputs: swapping the endpoints leaves the symmetric key unchanged.
#eval (List.range 300).all (fun x =>
  (List.range 300).all (fun y => decide (key (x, y) = key (y, x))))

theorem sound_OrientationTies : OrientationTies.Goal := by
  intro x y
  simp [key, Nat.min_comm, Nat.max_comm]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_UnorderedPair_OrientationTies
