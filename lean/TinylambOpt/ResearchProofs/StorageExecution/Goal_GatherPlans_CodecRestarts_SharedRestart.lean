import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `CodecRestarts.SharedRestart` — `∀ stride fee x y, x≤y → x/(stride+1)=y/(stride+1) →
  price stride fee x y = price stride fee y y`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_CodecRestarts_SharedRestart

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.CodecRestarts

-- probe: truth on small inputs
#eval (List.range 6).all (fun stride =>
  (List.range 5).all (fun fee =>
    (List.range 8).all (fun x =>
      (List.range 8).all (fun y =>
        x ≤ y → x/(stride+1) = y/(stride+1) →
          price stride fee x y = price stride fee y y))))

theorem sound_SharedRestart : CodecRestarts.SharedRestart.Goal := by
  intro stride fee x y hxy hquot
  unfold CodecRestarts.price
  rw [hquot]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_CodecRestarts_SharedRestart
