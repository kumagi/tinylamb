import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
import TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_ScanWithModularCarry

/-! REFUTE: verified DeltaLaneScan.ScanWithModularCarry. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DeltaLaneScan_ScanWithModularCarry
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.ScanWithModularCarry
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_ScanWithModularCarry.refute
end Nov26.Proof_DeltaLaneScan_ScanWithModularCarry
