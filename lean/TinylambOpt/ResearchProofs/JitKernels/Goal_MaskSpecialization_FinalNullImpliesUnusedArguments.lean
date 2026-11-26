import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_FinalNullImpliesUnusedArguments

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ FinalNullImpliesUnusedArguments.Goal := by
  intro h
  have hh := h [.null] (.arg 0) 0 .yes
  simp [answer, run, specialize] at hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_FinalNullImpliesUnusedArguments
