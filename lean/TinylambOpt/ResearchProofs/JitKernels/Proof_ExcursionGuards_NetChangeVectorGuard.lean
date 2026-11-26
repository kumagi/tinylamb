import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
An end-point (net-change) bound does NOT bound the intermediate excursions of
a chunk: with nonnegative total drift the running sum can still dip below the
lower guard mid-chunk. Counterexample: deltas `[-5, 10]` from accumulator 0
with guard interval `[-1, 7]`: every premise holds (total 5 ≥ 0, entry 0 is in
range, exit 5 is in range), yet the prefix after `-5` equals `-5 < -1`, so
`safe` fails. All obligations are closed ground arithmetic, checked by
`decide`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_NetChangeVectorGuard
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem refute : ¬ NetChangeVectorGuard.Goal := by
  intro h
  have this := h (-1) 7 0 [-5, 10] (by decide) (by decide) (by decide)
  exact absurd this (by decide)

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_NetChangeVectorGuard
