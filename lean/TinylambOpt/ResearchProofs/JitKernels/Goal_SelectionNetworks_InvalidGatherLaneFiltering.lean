import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! A suppressed first-stage lane shifts later ordinals; the composed selection vector is not equivalent. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_InvalidGatherLaneFiltering

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.InvalidGatherLaneFiltering

theorem refute : ¬ InvalidGatherLaneFiltering.Goal := by
  intro h
  have h' := h [10, 20] [99, 0] [0]
  simp [gather] at h'

end TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_InvalidGatherLaneFiltering
