import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

/-! Independent proof record for the MaskSpecialization.FinalNullImpliesUnusedArguments research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_MaskSpecialization_FinalNullImpliesUnusedArguments
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.FinalNullImpliesUnusedArguments
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ Goal := by
  intro h
  have hh := h [.null] (.arg 0) 0 .yes
  simp [answer, run, specialize] at hh

end Nov26.Proof_MaskSpecialization_FinalNullImpliesUnusedArguments
