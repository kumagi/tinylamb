import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace TinylambOpt.ResearchProofs.StorageExecution.BagKernels

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open MultiplicityRuns

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  -- need prove (live bag p).Perm (bag.expand p).eraseDups
  sorry

end TinylambOpt.ResearchProofs.StorageExecution.BagKernels
