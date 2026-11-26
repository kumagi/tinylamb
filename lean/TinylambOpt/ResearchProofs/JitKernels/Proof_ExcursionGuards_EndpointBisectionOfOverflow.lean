import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
Monotonicity of the guard predicate in the prefix length is FALSE without
hypotheses on the individual deltas: a later prefix can come back below the
overflow threshold, so "first index where the prefix sum overflows" is not an
upper-set and bisection on prefix endpoints can miss the fault. Counterexample:
threshold 5, accumulator 0, deltas `[10, -100]`: prefix 1 already overflows
(`10 > 5`) but prefix 2 does not (`-90 > 5` is false). Closed ground
obligations, checked by `decide`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_EndpointBisectionOfOverflow
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem refute : ¬ EndpointBisectionOfOverflow.Goal := by
  intro h
  have this := h 5 0 [10, -100] 1 2 (by decide) (by decide) (by decide)
  exact absurd this (by decide)

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_EndpointBisectionOfOverflow
