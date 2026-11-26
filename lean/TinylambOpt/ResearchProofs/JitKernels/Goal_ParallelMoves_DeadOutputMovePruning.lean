import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
Dead-output pruning removes moves whose destination is not live at the exit.
This file refutes the claim that such pruning is always safe: a removed move
may clobber a register that a later, live move reads, changing the value of the
live output.

Refuted by counterexample: with `live = [2]`, the dead move `(1,0)` writes the
register that the live move `(2,1)` reads.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_DeadOutputMovePruning

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ DeadOutputMovePruning.Goal := by
  intro h
  have heq := h (fun r => r) [(1, 0), (2, 1)] [2] 2 (by native_decide)
  exact (by native_decide :
    serial (fun r => r) ([(1, 0), (2, 1)].filter (fun m => [2].contains m.1)) 2 ≠
      serial (fun r => r) [(1, 0), (2, 1)] 2) heq

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_DeadOutputMovePruning
