import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
import TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_SkipIntervalDifference

/-! SOUND: verified DeltaLaneScan.SkipIntervalDifference. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DeltaLaneScan_SkipIntervalDifference
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.SkipIntervalDifference
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_SkipIntervalDifference.sound
end Nov26.Proof_DeltaLaneScan_SkipIntervalDifference
