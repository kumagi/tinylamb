import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

/-! Independent proof record for the MaskSpecialization.ConstantArmResultPrunesGuardFault research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false
set_option maxRecDepth 10000

namespace Nov26.Proof_MaskSpecialization_ConstantArmResultPrunesGuardFault
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.ConstantArmResultPrunesGuardFault

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ Goal := by
  intro h
  have hh := h [.fault 7] (.arg 0) (.lit .yes)
  simp [answer, run, after, slot] at hh

end Nov26.Proof_MaskSpecialization_ConstantArmResultPrunesGuardFault
