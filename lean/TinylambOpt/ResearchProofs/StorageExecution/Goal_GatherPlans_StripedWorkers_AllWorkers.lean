import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `StripedWorkers.AllWorkers` — `∀ k w a b, w+1≤pages (k+1) a b → price k w a b=span a
  b+w+1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_StripedWorkers_AllWorkers

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.StripedWorkers

-- truth probe on small inputs (only inputs satisfying the hypothesis matter)
#eval (List.range 5).all (fun k =>
  (List.range 5).all (fun w =>
    (List.range 12).all (fun a =>
      (List.range 12).all (fun b =>
        if w + 1 ≤ pages (k + 1) a b then
          price k w a b == span a b + w + 1
        else true))))

/-- Once the touched stripe count reaches the pool size, the activation term
    saturates at exactly `workers + 1`. -/
theorem sound_AllWorkers : StripedWorkers.AllWorkers.Goal := by
  intro k w a b h
  unfold price
  rw [Nat.min_eq_left h]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_StripedWorkers_AllWorkers
