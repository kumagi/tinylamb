import TinylambOpt.ResearchGoals.JitKernels.DispatchTables
import TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_JumpTableWrapForOutliers

/-! REFUTE: verified DispatchTables.JumpTableWrapForOutliers. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DispatchTables_JumpTableWrapForOutliers
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables.JumpTableWrapForOutliers
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_JumpTableWrapForOutliers.refute
end Nov26.Proof_DispatchTables_JumpTableWrapForOutliers
