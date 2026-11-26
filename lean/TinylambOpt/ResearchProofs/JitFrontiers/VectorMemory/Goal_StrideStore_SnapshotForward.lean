import Std
import TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideStore_SnapshotForward

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

theorem refute : ¬ StrideStore.SnapshotForward.Goal := by
  intro h
  have hv : valid StrideStore.kernel ⟨0,1,0,2,[],[]⟩ [1,2,3].length [0,1] := by simp [valid, StrideStore.kernel]
  have hc := h ⟨0,1,0,2,[],[]⟩ [1,2,3] [0,1] hv
  have hne : vector StrideStore.kernel ⟨0,1,0,2,[],[]⟩ [1,2,3] [0,1] ≠ scalar StrideStore.kernel ⟨0,1,0,2,[],[]⟩ [1,2,3] [0,1] := by native_decide
  exact hne hc

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideStore_SnapshotForward
