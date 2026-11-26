import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_ConstantArmResultPrunesGuardFault

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ ConstantArmResultPrunesGuardFault.Goal := by
  intro h
  have hh := h [.fault 7] (.arg 0) (.lit .yes)
  simp [answer, run, after, slot] at hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_ConstantArmResultPrunesGuardFault
