import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `DictionaryFault.WithinDictionary` — `∀ g f a b, a≤b → a/(g+1)=b/(g+1) → price g f a
  b=span a b+f`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_DictionaryFault_WithinDictionary

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.DictionaryFault

-- probe: truth on small inputs
#eval (List.range 8).all (fun g =>
  (List.range 5).all (fun f =>
    (List.range 10).all (fun a =>
      (List.range 10).all (fun b =>
        a ≤ b → a/(g+1) = b/(g+1) →
          price g f a b = span a b + f))))

theorem sound_WithinDictionary : DictionaryFault.WithinDictionary.Goal := by
  intro g f a b hab hbucket
  simp [DictionaryFault.price, pages, hbucket]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_DictionaryFault_WithinDictionary
