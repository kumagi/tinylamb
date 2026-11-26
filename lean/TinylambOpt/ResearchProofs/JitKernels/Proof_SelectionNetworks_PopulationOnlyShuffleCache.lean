import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace Nov26.Proof_SelectionNetworks_PopulationOnlyShuffleCache
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.PopulationOnlyShuffleCache
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks PopulationOnlyShuffleCache

/--
`PopulationOnlyShuffleCache.Goal` claims that two masks of equal length and equal population
count compress a value vector identically, so a shuffle control could be cached by popcount
alone. This is false: the positions of the set bits matter. Counterexample: `xs = [1,2]`,
`a = [true,false]`, `b = [false,true]`; both masks have length 2 and one set bit, yet the
compressions are `[1]` and `[2]`.
-/
theorem refute : ¬ Goal := by
  intro h
  have hcontra : compress [1,2] [true,false] ≠ compress [1,2] [false,true] := by
    decide
  exact hcontra (h [1,2] [true,false] [false,true] (by decide) (by decide) (by decide))

end Nov26.Proof_SelectionNetworks_PopulationOnlyShuffleCache