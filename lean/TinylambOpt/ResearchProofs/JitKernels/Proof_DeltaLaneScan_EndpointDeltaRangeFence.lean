import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
import TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_EndpointDeltaRangeFence

/-! SOUND: verified DeltaLaneScan.EndpointDeltaRangeFence. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DeltaLaneScan_EndpointDeltaRangeFence
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.EndpointDeltaRangeFence
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_EndpointDeltaRangeFence.sound
end Nov26.Proof_DeltaLaneScan_EndpointDeltaRangeFence
