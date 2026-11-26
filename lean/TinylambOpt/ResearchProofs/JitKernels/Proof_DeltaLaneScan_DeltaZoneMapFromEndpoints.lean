import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
import TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_DeltaZoneMapFromEndpoints

/-! REFUTE: verified DeltaLaneScan.DeltaZoneMapFromEndpoints. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DeltaLaneScan_DeltaZoneMapFromEndpoints
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.DeltaZoneMapFromEndpoints
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_DeltaZoneMapFromEndpoints.refute
end Nov26.Proof_DeltaLaneScan_DeltaZoneMapFromEndpoints
