import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

/-!
Acceptance under WHERE collapses UNKNOWN and fault into the same rejection bit,
but NOT distinguishes them: UNKNOWN stays UNKNOWN while a fault is re-observed.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_FilterAcceptanceForNestedNegation

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.FilterAcceptanceForNestedNegation.Goal := by
  intro h
  have h' := h [] (.lit .no) (.lit .null) (by native_decide)
  have hne : ¬ ((answer [] (.neg (.lit .no)) == .yes) =
      (answer [] (.neg (.lit .null)) == .yes)) := by
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_FilterAcceptanceForNestedNegation
