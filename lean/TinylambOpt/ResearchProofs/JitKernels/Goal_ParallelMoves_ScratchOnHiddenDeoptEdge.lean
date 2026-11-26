import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
A scratch register used to break a two-cycle swap is not observable by the
parallel bundle on the hot path. This file refutes the stronger claim that the
scratch register is also dead on a side-exit reconstruction: the serial
rotation leaves the old value of `a` in `tmp`, while the parallel bundle leaves
the original value of `tmp` there.

Refuted by counterexample: identity register file, `a = 1`, `b = 2`, `tmp = 3`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_ScratchOnHiddenDeoptEdge

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ ScratchOnHiddenDeoptEdge.Goal := by
  intro h
  have heq := h (fun r => r) 1 2 3 (by omega) (by omega) (by omega)
  exact (by native_decide :
    serial (fun r => r) [(3, 1), (1, 2), (2, 3)] 3 ≠
      parallelCopy (fun r => r) [(1, 2), (2, 1)] 3) heq

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_ScratchOnHiddenDeoptEdge
