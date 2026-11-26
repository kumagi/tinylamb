import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_ManyToOne

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

-- Sanity checks on small inputs (including an already-present key and a fresh key).
#eval bag.mass ([5, 3, 5] ++ [7], [0, 1, 3, 9, 2]) 7
#eval bag.mass ([5, 3, 5], [0, 1, 3, 9, 2]) 7 + [0, 1, 3, 9, 2].count 3
example : bag.mass ([5, 3, 5] ++ [7], [0, 1, 3, 9, 2]) 7 =
    bag.mass ([5, 3, 5], [0, 1, 3, 9, 2]) 7 + [0, 1, 3, 9, 2].count 3 := by decide
example : bag.mass ([3, 3, 3] ++ [3], [1, 1, 2, 3]) 3 =
    bag.mass ([3, 3, 3], [1, 1, 2, 3]) 3 + [1, 1, 2, 3].count 3 := by decide

theorem sound_ManyToOne : DictionaryAliases.ManyToOne.Goal := by
  intro d ids v
  unfold bag
  simp only
  rw [List.zipIdx_append]
  simp [List.sum_append]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_ManyToOne
