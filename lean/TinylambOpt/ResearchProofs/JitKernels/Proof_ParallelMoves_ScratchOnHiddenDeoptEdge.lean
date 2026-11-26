import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
**Refuted: the scratch register used to break a swap cycle is not, in
general, free to omit from a side-exit reconstruction.**
The claim says that evaluating the scratch rotation
`serial s [(tmp, a), (a, b), (b, tmp)]` *at* `tmp` already agrees with the
pure parallel swap `parallelCopy s [(a, b), (b, a)]` at `tmp`, so the
side-exit could reconstruct registers without restoring `tmp`'s entry value.
It fails: with the identity register file and `a = 1, b = 2, tmp = 3`, the
rotation leaves `tmp` holding `s a = 1` (the first move stored `a`'s entry
value into `tmp` and nothing overwrote it), whereas the swap leaves `tmp` at
its entry value `3`. A deopt edge that resumes from the scratch-restored
frame would therefore observe a stale value in the scratch slot; the exit
must still reconstruct `tmp` from the entry snapshot.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_Scratch
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem refute : ¬ ScratchOnHiddenDeoptEdge.Goal := by
  intro h
  have hh := h (fun r => r) 1 2 3 (by decide) (by decide) (by decide)
  exact absurd hh (by decide)

end TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_Scratch
