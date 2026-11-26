import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
A scratch register used to break a swap is unobservable on the hot path but is
observable on a side exit: the serial rotation leaves the old value of `a` in
`tmp`, while the parallel bundle leaves the original value of `tmp` there.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_ParallelMoves_ScratchOnHiddenDeoptEdge
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ ScratchOnHiddenDeoptEdge.Goal := by
  intro h
  have heq := h (fun r => r) 1 2 3 (by omega) (by omega) (by omega)
  exact (by native_decide :
    serial (fun r => r) [(3, 1), (1, 2), (2, 3)] 3 ≠
      parallelCopy (fun r => r) [(1, 2), (2, 1)] 3) heq

end Nov26.Proof_ParallelMoves_ScratchOnHiddenDeoptEdge
