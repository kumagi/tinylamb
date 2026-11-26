import Std
import TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_SnapshotForward

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

theorem refute : ¬ RingWindow.SnapshotForward.Goal := by
  intro h
  have hv : valid RingWindow.kernel ⟨0,1,1,1,[],[]⟩ [1,2,3].length [0,1] := by simp [valid, RingWindow.kernel]
  have hc := h ⟨0,1,1,1,[],[]⟩ [1,2,3] [0,1] hv
  have hne : vector RingWindow.kernel ⟨0,1,1,1,[],[]⟩ [1,2,3] [0,1] ≠ scalar RingWindow.kernel ⟨0,1,1,1,[],[]⟩ [1,2,3] [0,1] := by native_decide
  exact hne hc

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_SnapshotForward
