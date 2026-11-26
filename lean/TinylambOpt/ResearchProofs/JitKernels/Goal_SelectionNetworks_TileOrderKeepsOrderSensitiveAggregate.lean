import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! Tile order changes which selected row is first when the input selection is not tile-ordered. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_TileOrderKeepsOrderSensitiveAggregate

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.TileOrderKeepsOrderSensitiveAggregate

theorem refute : ¬ TileOrderKeepsOrderSensitiveAggregate.Goal := by
  intro h
  have h' := h [10, 20] [1, 0] 1 (by omega)
  simp [gather, tile] at h'

end TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_TileOrderKeepsOrderSensitiveAggregate
