import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_FilterAcceptanceForNestedNegation

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ FilterAcceptanceForNestedNegation.Goal := by
  intro h
  have hh := h [] (.lit .null) (.lit .no)
  simp [answer, run, TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA.flip] at hh
  have hn : (V.null == V.yes) = false := by decide
  have hno : (V.no == V.yes) = false := by decide
  have hy : (V.yes == V.yes) = true := by decide
  simp [hn, hno, hy] at hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_FilterAcceptanceForNestedNegation
