import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
Refutation: checking only `c < 128 ∧ c % 16 < 10` does not verify ASCII
digit-ness. The byte `0` satisfies `0 < 128 ∧ 0 % 16 < 10`, but it is not an
ASCII digit (`48 ≤ 0` fails), so `digits [0] = false`. Skipping the
high-nibble check lets non-digit control bytes through.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_PackedLowNibblesValidateDigits
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ PackedLowNibblesValidateDigits.Goal := by
  intro h
  exact absurd (h [0] (by decide) (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_PackedLowNibblesValidateDigits
