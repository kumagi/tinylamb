import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/-!
Null-producing packets before the first fault are skipped by `prefixOut`, so the
number of materialized outputs need not equal the physical ordinal of the fault.
This counterexample keeps a SQL NULL row before an error row.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_SuccessfulNullLaneProgress

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.SuccessfulNullLaneProgress.Goal := by
  intro h
  have h' := h ([.ok none, .error 3] : List Cell) 1 3 (by native_decide)
  have hne : ¬ ((prefixOut ([.ok none, .error 3] : List Cell)).length = 1) := by native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_SuccessfulNullLaneProgress
