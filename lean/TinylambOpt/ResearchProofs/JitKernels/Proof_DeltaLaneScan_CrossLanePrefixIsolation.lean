import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
import TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_CrossLanePrefixIsolation

/-! REFUTE: verified DeltaLaneScan.CrossLanePrefixIsolation. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DeltaLaneScan_CrossLanePrefixIsolation
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.CrossLanePrefixIsolation
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_CrossLanePrefixIsolation.refute
end Nov26.Proof_DeltaLaneScan_CrossLanePrefixIsolation
