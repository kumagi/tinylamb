import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.BailoutPackets_LowestSourceOrdinalIsFirstFault
open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/--
`LowestSourceOrdinalIsFirstFault.Goal` claims that the first reported fault lane
also carries the smallest physical source ordinal among all faulting selected
rows. This is false: the packet reports the first failing *lane*, so a selection
vector that reverses the physical order reports the later row first.
Witness: `xs = [.error 7, .error 3]`, `sel = [1, 0]`; the first fault lane is
`j = 0` (row 1), yet row 0 faults too and `sel[0]?.getD 0 = 1 ≤ 0` fails.
-/
theorem refute : ¬ LowestSourceOrdinalIsFirstFault.Goal := by
  intro h
  exact absurd
    (h [(.error 7), (.error 3)] [1, 0] 0 3 rfl 1 0 7 rfl rfl)
    (by decide)

end TinylambOpt.ResearchProofs.JitKernels.BailoutPackets_LowestSourceOrdinalIsFirstFault
