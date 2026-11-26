import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
import TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_HorizontalScanChunkSeed

/-! SOUND: verified DeltaLaneScan.HorizontalScanChunkSeed. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DeltaLaneScan_HorizontalScanChunkSeed
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.HorizontalScanChunkSeed
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_HorizontalScanChunkSeed.sound
end Nov26.Proof_DeltaLaneScan_HorizontalScanChunkSeed
