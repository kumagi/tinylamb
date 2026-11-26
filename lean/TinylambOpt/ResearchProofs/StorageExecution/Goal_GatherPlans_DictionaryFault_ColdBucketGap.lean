import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_DictionaryFault_ColdBucketGap

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.DictionaryFault

-- probe: truth on small inputs
#eval (List.range 8).all (fun g =>
  (List.range 5).all (fun f =>
    (List.range 10).all (fun a =>
      (List.range 10).all (fun b =>
        (List.range 12).all (fun c =>
          a ≤ b → b < c → b/(g+1) = c/(g+1) →
            price g f a c - price g f a b = c - b)))))

theorem sound_ColdBucketGap : DictionaryFault.ColdBucketGap.Goal := by
  intro g f a b c hab hbc hbucket
  unfold DictionaryFault.price span pages
  have hpage : c / (g + 1) - a / (g + 1) + 1 =
      b / (g + 1) - a / (g + 1) + 1 := by
    rw [hbucket]
  rw [hpage]
  have hspan : c + 1 - a = (b + 1 - a) + (c - b) := by
    omega
  rw [hspan]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_DictionaryFault_ColdBucketGap
