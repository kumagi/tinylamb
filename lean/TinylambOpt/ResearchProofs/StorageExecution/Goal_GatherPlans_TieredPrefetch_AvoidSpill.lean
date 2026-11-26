import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `TieredPrefetch.AvoidSpill` — `∀ lim fee a n, 0<n → n≤lim → lim<2*n → price lim fee a
  (a+n-1)+price lim fee (a+n) (a+2*n-1) ≤ price lim fee a (a+2*n-1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_AvoidSpill

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.TieredPrefetch

-- Truth check on small inputs satisfying 0 < n, n ≤ lim, lim < 2*n.
#eval ((List.range 9).all (fun lim =>
        (List.range 6).all (fun fee =>
          (List.range 6).all (fun a =>
            (List.range 9).all (fun n =>
              0 < n → n ≤ lim → lim < 2*n →
                price lim fee a (a+n-1) + price lim fee (a+n) (a+2*n-1) ≤
                  price lim fee a (a+2*n-1))))))

theorem sound_AvoidSpill : TieredPrefetch.AvoidSpill.Goal := by
  intro lim fee a n hn hnle hlim
  have hs1 : a + n - 1 + 1 - a = n := by omega
  have hs2 : a + 2 * n - 1 + 1 - (a + n) = n := by omega
  have hs3 : a + 2 * n - 1 + 1 - a = 2 * n := by omega
  have hnot : ¬ lim < n := by omega
  unfold price span
  simp [hs1, hs2, hs3, hnot, hlim] <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_AvoidSpill
