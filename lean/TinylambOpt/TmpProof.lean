import TinylambOpt.ResearchGoals.StorageExecution.BagKernels
namespace Tmp
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open MultiplicityRuns

theorem t : DistinctOutput.Goal := by
  intro p
  simp [live, bag]
