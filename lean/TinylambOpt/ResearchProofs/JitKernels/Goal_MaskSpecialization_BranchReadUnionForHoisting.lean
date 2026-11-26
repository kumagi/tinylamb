import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_BranchReadUnionForHoisting

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ BranchReadUnionForHoisting.Goal := by
  intro h
  have hh := h [.fault 5, .fault 9] (.arg 0) (.arg 1) (.arg 1) 1 9
  simp [answer, reads, run, after, slot] at hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_BranchReadUnionForHoisting
