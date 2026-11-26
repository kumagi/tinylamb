import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
import TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_DeferredWrapThenRangeCheck

/-! REFUTE: verified DecimalChunks.DeferredWrapThenRangeCheck. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DecimalChunks_DeferredWrapThenRangeCheck
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.DeferredWrapThenRangeCheck
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_DeferredWrapThenRangeCheck.refute
end Nov26.Proof_DecimalChunks_DeferredWrapThenRangeCheck
