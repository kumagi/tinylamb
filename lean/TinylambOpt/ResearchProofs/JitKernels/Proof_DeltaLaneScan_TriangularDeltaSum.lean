import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
import TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_TriangularDeltaSum

/-! SOUND: verified DeltaLaneScan.TriangularDeltaSum. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DeltaLaneScan_TriangularDeltaSum
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.TriangularDeltaSum
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_TriangularDeltaSum.sound
end Nov26.Proof_DeltaLaneScan_TriangularDeltaSum
