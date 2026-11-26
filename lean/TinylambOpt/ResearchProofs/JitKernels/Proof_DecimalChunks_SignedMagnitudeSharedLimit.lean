import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
Refutation: signed range checks are asymmetric (`-2^w ≤ v ∧ v < 2^w`), so the
negative endpoint `-2^w` parses while `+2^w` does not. With `w = 4` the digit
string "16" has magnitude 16 = 2^4: `parse 4 true [49,54] = some (-16)` accepts
it, but `parse 4 false [49,54] = none` rejects it. A shared unsigned cutoff
without the special negative endpoint changes which values parse.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_SignedMagnitudeSharedLimit
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ SignedMagnitudeSharedLimit.Goal := by
  intro h
  exact absurd (h 4 [49,54]) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_SignedMagnitudeSharedLimit
