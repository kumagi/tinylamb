import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_TraceSetAsResultCacheKey

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ TraceSetAsResultCacheKey.Goal := by
  intro h
  have hh := h [.yes] [.no] (.arg 0)
  simp [answer, reads, run] at hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_TraceSetAsResultCacheKey
