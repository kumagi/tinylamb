import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
import TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_ExactChunkOverflowDivision

/-! SOUND: verified DecimalChunks.ExactChunkOverflowDivision. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DecimalChunks_ExactChunkOverflowDivision
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.ExactChunkOverflowDivision
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_ExactChunkOverflowDivision.sound
end Nov26.Proof_DecimalChunks_ExactChunkOverflowDivision
