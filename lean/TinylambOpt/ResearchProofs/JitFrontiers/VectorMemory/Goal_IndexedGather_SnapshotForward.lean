import Std
import TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedGather_SnapshotForward

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

theorem refute : ¬ IndexedGather.SnapshotForward.Goal := by
  intro h
  have hv : valid IndexedGather.kernel ⟨0,1,1,3,[0,1],[]⟩ [1,2,3].length [0,1] := by simp [valid, IndexedGather.kernel]
  have hc := h ⟨0,1,1,3,[0,1],[]⟩ [1,2,3] [0,1] hv
  have hne : vector IndexedGather.kernel ⟨0,1,1,3,[0,1],[]⟩ [1,2,3] [0,1] ≠ scalar IndexedGather.kernel ⟨0,1,1,3,[0,1],[]⟩ [1,2,3] [0,1] := by native_decide
  exact hne hc

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedGather_SnapshotForward
