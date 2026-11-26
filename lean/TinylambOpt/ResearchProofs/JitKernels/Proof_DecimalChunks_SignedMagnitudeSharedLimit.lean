import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
import TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_SignedMagnitudeSharedLimit

/-! REFUTE: verified DecimalChunks.SignedMagnitudeSharedLimit. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DecimalChunks_SignedMagnitudeSharedLimit
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.SignedMagnitudeSharedLimit
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_SignedMagnitudeSharedLimit.refute
end Nov26.Proof_DecimalChunks_SignedMagnitudeSharedLimit
