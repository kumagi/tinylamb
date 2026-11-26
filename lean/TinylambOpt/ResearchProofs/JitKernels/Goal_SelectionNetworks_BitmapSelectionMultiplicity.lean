import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! A repeated ordinal contributes its value twice to a SUM gather but only once to a membership bitmap. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_BitmapSelectionMultiplicity

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.BitmapSelectionMultiplicity

theorem refute : ¬ BitmapSelectionMultiplicity.Goal := by
  intro h
  have h' := h [5] [0, 0]
  simp [gather, compress, positions] at h'

end TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_BitmapSelectionMultiplicity
