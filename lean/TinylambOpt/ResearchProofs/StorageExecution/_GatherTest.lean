import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

namespace Test
set_option maxHeartbeats 0
set_option maxRecDepth 10000
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
example : QuadraticDecode.MergeNeighbors.Goal := by
  intro f a m b ham hmb
  simp only [QuadraticDecode.price, span]
  have hsab : b + 1 - a = (m + 1 - a) + (b - m) := by omega
  have hsmb : b + 1 - (m + 1) = b - m := by omega
  rw [hsab, hsmb]
  grind
