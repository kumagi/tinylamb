import TinylambOpt.ResearchGoals.JitKernels.DispatchTables
import TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_NullableResultFallthrough

/-! REFUTE: verified DispatchTables.NullableResultFallthrough. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DispatchTables_NullableResultFallthrough
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables.NullableResultFallthrough
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_NullableResultFallthrough.refute
end Nov26.Proof_DispatchTables_NullableResultFallthrough
