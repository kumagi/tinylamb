import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! Two masks with the same population count can select different values; positions, not the count, matter. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_PopulationOnlyShuffleCache

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.PopulationOnlyShuffleCache

theorem refute : ¬ PopulationOnlyShuffleCache.Goal := by
  intro h
  have h' := h [10, 20] [true, false] [false, true] (by simp) (by simp) (by simp)
  simp [compress, gather, positions] at h'

end TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_PopulationOnlyShuffleCache
