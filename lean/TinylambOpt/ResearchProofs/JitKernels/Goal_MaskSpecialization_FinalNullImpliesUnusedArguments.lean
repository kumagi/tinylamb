import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

-- Refutation: substituting a non-NULL literal for an actually-read NULL slot changes the result.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_FinalNullImpliesUnusedArguments

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.FinalNullImpliesUnusedArguments.Goal := by
  intro h
  have c := h ([V.null] : List V) (.arg 0) 0 .yes (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.answer
      ([V.null] : List V)
      (TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.specialize 0 .yes (.arg 0)) = .null)) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_FinalNullImpliesUnusedArguments
