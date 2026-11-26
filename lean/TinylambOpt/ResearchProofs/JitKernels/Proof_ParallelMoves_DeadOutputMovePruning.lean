import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
**Refuted: a move whose destination is dead at the exit cannot always be
pruned from a serial bundle.**
The claim says that filtering out moves whose destinations are not in the
live set preserves the serial result on every live register. It fails: with
the identity register file, `ms = [(1, 0), (2, 1)]` and `live = [2]`, the
second move reads register 1, which the *first* (dead-destination) move just
wrote. Dropping `(1, 0)` changes where `(2, 1)` reads from, so the live
register 2 observes 0 instead of 1. A register allocator may only drop dead
outputs when they do not feed a surviving move in the same bundle; the
general pruning the claim allows would change query-visible results at
OSR/trace-stitching exits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_DeadOutput
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ DeadOutputMovePruning.Goal := by
  intro h
  have hh := h (fun r => r) [(1, 0), (2, 1)] [2] 2 (by decide)
  exact absurd hh (by decide)

end TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_DeadOutput
