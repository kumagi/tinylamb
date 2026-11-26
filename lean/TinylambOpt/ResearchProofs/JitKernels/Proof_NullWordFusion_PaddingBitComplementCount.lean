import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Refuted: padding-complement count without a positive-width guard.** When the
padding-clearness premise holds vacuously (`n = 0`, `m = 0`), the padding bits
are clear but `64 - popcount m = 64` overshoots the true non-null row count 0.
SQL execution may complement the full-word popcount only once the kernel also
knows the window is full (or counts within `n`), so this unqualified version is
refuted with an explicit witness.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_PaddingBitComplementCount
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem refute : ¬ PaddingBitComplementCount.Goal := by
  intro h
  have hpad : (0 : W) &&& ~~~(window 0) = 0 := by decide
  have hh := h 0 (0 : W) (by omega) hpad
  exact (by decide :
    ¬ (ords (valid 0 (0 : W))).length = 64 - (ords (0 : W)).length) hh

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_PaddingBitComplementCount
