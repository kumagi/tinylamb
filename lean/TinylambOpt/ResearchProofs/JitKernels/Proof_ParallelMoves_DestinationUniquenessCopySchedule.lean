import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
**Refuted: distinct destinations alone do not license a straight serial copy
sequence at a join.**
The claim says that for any move bundle whose destinations are pairwise
distinct, executing the moves serially (each read seeing the writes of
earlier moves) equals the parallel copy semantics (every read from the entry
snapshot). It fails: with `ms = [(1, 2), (3, 1)]` over the identity register
file, move `(3, 1)` reads register 1 *after* the first move wrote 2 into it,
so the serial result maps 3 to 2 while `parallelCopy` maps 3 to 1. A JIT
transfer at a trace join or PHI lowering site must therefore additionally
check that no source is clobbered by an earlier destination (or use the
cycle-breaking scratch scheme), not just that destinations are unique;
otherwise query results observable through the transferred registers change.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_DestinationUniqueness
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ DestinationUniquenessCopySchedule.Goal := by
  intro h
  have hh := h [(1, 2), (3, 1)] (fun r => r) (by decide)
  exact (by decide :
      serial (fun r => r) [(1, 2), (3, 1)] 3 ≠ parallelCopy (fun r => r) [(1, 2), (3, 1)] 3)
      (congrFun hh 3)

end TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_DestinationUniqueness
