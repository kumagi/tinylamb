import Std
import TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_SnapshotForward

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

theorem refute : ¬ PackedBits.SnapshotForward.Goal := by
  intro h
  have hv : valid PackedBits.kernel ⟨0,1,1,2,[],[]⟩ ((List.range 66).map (fun i => i+1)).length [0,64] := by simp [valid, PackedBits.kernel]
  have hc := h ⟨0,1,1,2,[],[]⟩ ((List.range 66).map (fun i => i+1)) [0,64] hv
  have hne : vector PackedBits.kernel ⟨0,1,1,2,[],[]⟩ ((List.range 66).map (fun i => i+1)) [0,64] ≠ scalar PackedBits.kernel ⟨0,1,1,2,[],[]⟩ ((List.range 66).map (fun i => i+1)) [0,64] := by native_decide
  exact hne hc

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_SnapshotForward
