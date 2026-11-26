import Std
import TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_NeighborSum_SnapshotForward

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

theorem refute : ¬ NeighborSum.SnapshotForward.Goal := by
  intro h
  have hv : valid NeighborSum.kernel ⟨0,1,1,3,[],[]⟩ [1,2,3].length [0,1] := by simp [valid, NeighborSum.kernel]
  have hc := h ⟨0,1,1,3,[],[]⟩ [1,2,3] [0,1] hv
  have hne : vector NeighborSum.kernel ⟨0,1,1,3,[],[]⟩ [1,2,3] [0,1] ≠ scalar NeighborSum.kernel ⟨0,1,1,3,[],[]⟩ [1,2,3] [0,1] := by native_decide
  exact hne hc

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_NeighborSum_SnapshotForward
