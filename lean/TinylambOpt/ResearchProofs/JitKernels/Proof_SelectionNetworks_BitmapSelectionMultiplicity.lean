import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace Nov26.Proof_SelectionNetworks_BitmapSelectionMultiplicity
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.BitmapSelectionMultiplicity
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks BitmapSelectionMultiplicity

/--
`BitmapSelectionMultiplicity.Goal` claims a gathered SUM equals the SUM over a membership bitmap.
This is false: a bitmap records whether an ordinal is selected at all, so repeated gathered lanes
contribute only once. Counterexample: `xs = [1,2]`, `sel = [0,0]`; the gather sums `1+1 = 2` while
the bitmap `[true,false]` sums to `1`.
-/
theorem refute : ¬ Goal := by
  intro h
  have hcontra : (gather [1,2] [0,0]).sum ≠
      (compress [1,2] ((List.range 2).map [0,0].contains)).sum := by
    decide
  exact hcontra (h [1,2] [0,0])

end Nov26.Proof_SelectionNetworks_BitmapSelectionMultiplicity