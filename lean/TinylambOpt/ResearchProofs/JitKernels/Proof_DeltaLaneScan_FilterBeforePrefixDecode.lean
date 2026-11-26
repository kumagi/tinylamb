import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
import TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_FilterBeforePrefixDecode

/-! REFUTE: verified DeltaLaneScan.FilterBeforePrefixDecode. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DeltaLaneScan_FilterBeforePrefixDecode
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.FilterBeforePrefixDecode
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_FilterBeforePrefixDecode.refute
end Nov26.Proof_DeltaLaneScan_FilterBeforePrefixDecode
