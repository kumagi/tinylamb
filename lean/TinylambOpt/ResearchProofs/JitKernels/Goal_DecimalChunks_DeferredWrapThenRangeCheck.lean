import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_DeferredWrapThenRangeCheck

/-- A wrapped magnitude can be small even when the true decimal value is out of range:
`4 = 2^(1+1)` wraps to zero but `parse 1 false "4"` must fail. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.DeferredWrapThenRangeCheck.Goal := by
  intro h
  have c := h [52] 1 (by native_decide) (by native_decide) (by decide)
  have bad : ¬ (TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.parse 1 false [52]).isSome := by
    native_decide
  exact bad c

end TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_DeferredWrapThenRangeCheck
