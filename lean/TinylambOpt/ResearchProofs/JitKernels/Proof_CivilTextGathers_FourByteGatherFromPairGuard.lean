import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.CivilTextGathers_FourByteGatherFromPairGuard
open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/--
`FourByteGatherFromPairGuard.Goal` claims the two-byte pair guard
(`start+off+2 ≤ limit`) licenses a four-byte gather (`start+off+4 ≤ limit`).
This is false: the guard bounds only the pair's last index, two bytes short.
Witness: `start = off = 0`, `limit = 2`.
-/
theorem refute : ¬ FourByteGatherFromPairGuard.Goal := by
  intro h
  exact absurd (h 0 2 0 (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.CivilTextGathers_FourByteGatherFromPairGuard
