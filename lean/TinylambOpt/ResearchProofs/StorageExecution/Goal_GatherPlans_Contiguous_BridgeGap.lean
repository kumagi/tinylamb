import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_BridgeGap

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Contiguous

-- probe: truth on small inputs
#eval (List.range 8).all (fun fee =>
  (List.range 8).all (fun x =>
    (List.range 8).all (fun y =>
      x < y → (price fee x y ≤ price fee x x + price fee y y ↔ y - x - 1 ≤ fee))))

theorem sound_BridgeGap : Contiguous.BridgeGap.Goal := by
  intro fee x y hxy
  unfold Contiguous.price span
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_BridgeGap
