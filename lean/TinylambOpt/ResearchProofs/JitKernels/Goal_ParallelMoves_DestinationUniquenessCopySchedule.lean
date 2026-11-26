import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
Parallel-move bundles at a join are lowered into a straight sequence of machine
moves. This file refutes the claim that uniqueness of destinations alone is a
sufficient no-alias check: an earlier destination can still be the source of a
later move, so the serial sequence reads a clobbered value.

Refuted by counterexample: `[(1,2),(3,1)]` has distinct destinations `1` and
`3`, but the first move overwrites register `1`, which the second move reads.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_DestinationUniquenessCopySchedule

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ DestinationUniquenessCopySchedule.Goal := by
  intro h
  have heq := h [(1, 2), (3, 1)] (fun r => r) (by native_decide)
  exact (by native_decide : serial (fun r => r) [(1, 2), (3, 1)] 3 ≠
    parallelCopy (fun r => r) [(1, 2), (3, 1)] 3) (congrFun heq 3)

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_DestinationUniquenessCopySchedule
