import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_ScratchOnHiddenDeoptEdge

/-- The scratch rotation leaves the old scratch value at the scratch register,
while the parallel swap leaves it untouched; these differ on a hidden exit. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ParallelMoves.ScratchOnHiddenDeoptEdge.Goal := by
  intro h
  have c := h (fun r => r) 1 2 3 (by decide) (by decide) (by decide)
  have bad :
      TinylambOpt.ResearchGoals.JitKernels.ParallelMoves.serial (fun r => r)
          [(3, 1), (1, 2), (2, 3)] 3 ≠
        TinylambOpt.ResearchGoals.JitKernels.ParallelMoves.parallelCopy (fun r => r)
          [(1, 2), (2, 1)] 3 := by
    native_decide
  exact bad c

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_ScratchOnHiddenDeoptEdge
