import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.BailoutPackets_SuccessfulNullLaneProgress
open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/--
`SuccessfulNullLaneProgress.Goal` claims the number of materialized outputs equals
the reported fault ordinal. This is false: a NULL lane before the fault materializes
no output, so the output buffer shrinks below the fault ordinal.
Witness: `xs = [.ok none, .error 5]` faults at ordinal 1 but yields 0 outputs.
-/
theorem refute : ¬ SuccessfulNullLaneProgress.Goal := by
  intro h
  exact absurd (h [.ok none, .error 5] 1 5 rfl) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.BailoutPackets_SuccessfulNullLaneProgress
