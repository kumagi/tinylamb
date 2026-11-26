import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace Nov26.Proof_SelectionNetworks_InvalidGatherLaneFiltering
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.InvalidGatherLaneFiltering
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks InvalidGatherLaneFiltering

/--
`InvalidGatherLaneFiltering.Goal` claims that fusing two gathers by composing their selection
vectors is always valid, even when a first-stage lane held an out-of-range ordinal. This is
false: suppressing an invalid lane in stage one shifts every later lane of the intermediate
result, so composing ordinal vectors against the unshifted stage-one input picks different rows.
Counterexample: `xs = [1,2]`, `a = [0,5,1]` (lane `5` is suppressed), `b = [2]`:
the fused side gathers `(gather [1,2] [0,5,1])[2]? = none` and yields `[]`, while the composed
side gathers ordinal `a[2] = 1` and yields `[2]`.
-/
theorem refute : ¬ Goal := by
  intro h
  have hcontra : gather (gather [1,2] [0,5,1]) [2] ≠ gather [1,2] (gather [0,5,1] [2]) := by
    decide
  exact hcontra (h [1,2] [0,5,1] [2])

end Nov26.Proof_SelectionNetworks_InvalidGatherLaneFiltering