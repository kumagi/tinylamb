import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
import TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_SparseGatherWeightedPrefix

/-! SOUND: verified DeltaLaneScan.SparseGatherWeightedPrefix. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DeltaLaneScan_SparseGatherWeightedPrefix
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.SparseGatherWeightedPrefix
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_SparseGatherWeightedPrefix.sound
end Nov26.Proof_DeltaLaneScan_SparseGatherWeightedPrefix
