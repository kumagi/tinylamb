import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "round down the multiplier and add one to the dividend". With the
truncated (round-down) multiplier `2^s / d` the compensated increment is not exact in
general: for `x = 5`, `d = 5`, `s = 3` it gives `(5+1)*(8/5)/8 = 6*1/8 = 0` while
`5/5 = 1`. The kernel must keep the correction (or a different compensation) instead of
an unconditional dividend increment.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_RoundedDownAddOne
open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem refute : ¬ RoundedDownAddOne.Goal := by
  intro h
  have h' := h 5 5 3 (by omega) (by omega)
  exact absurd h' (by decide)

end TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_RoundedDownAddOne
