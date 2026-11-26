import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_EndpointBisectionOfOverflow

/-- Exceeding the bound at an earlier prefix does not imply the later prefix
also exceeds it: `[10,-10]` with bound 5 crosses at ordinal 1 but ends at 0. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.EndpointBisectionOfOverflow.Goal := by
  intro h
  let xs : List Int := [10, -10]
  have c := h 5 0 xs 1 2 (by native_decide) (by native_decide) (by native_decide)
  have bad : ¬ ((5 : Int) < 0 + (xs.take 2).sum) := by native_decide
  exact bad c

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_EndpointBisectionOfOverflow
