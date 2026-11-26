import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
import TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_LeadingZeroLengthGuard

/-! REFUTE: verified DecimalChunks.LeadingZeroLengthGuard. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DecimalChunks_LeadingZeroLengthGuard
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.LeadingZeroLengthGuard
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_LeadingZeroLengthGuard.refute
end Nov26.Proof_DecimalChunks_LeadingZeroLengthGuard
