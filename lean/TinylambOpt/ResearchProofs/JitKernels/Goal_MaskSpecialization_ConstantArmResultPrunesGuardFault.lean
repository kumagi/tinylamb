import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_ConstantArmResultPrunesGuardFault

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

/-! The predicate itself can fault before either identical arm is reached. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.ConstantArmResultPrunesGuardFault.Goal := by
  rw [ConstantArmResultPrunesGuardFault.Goal]
  intro h
  have hh := h [.fault 7] (.arg 0) (.lit .yes)
  simp [answer, run, after] at hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_ConstantArmResultPrunesGuardFault
