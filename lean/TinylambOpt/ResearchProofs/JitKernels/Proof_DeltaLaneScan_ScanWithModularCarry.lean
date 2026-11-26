import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
Refutation: endpoints in range do not keep intermediate decoded values in a
narrow carry. With `base = 0`, `w = 3`, `ds = [10,-10]`: the initial value 0
and final value `0 + 10 + (-10) = 0` both lie in `[0, 8)`, yet
`values 0 [10,-10] = [10, 0]` and the intermediate row 10 overflows `2^3`.
Widening is only safe with a per-step bound, not from endpoints alone.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_ScanWithModularCarry
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem refute : ¬ ScanWithModularCarry.Goal := by
  intro h
  have h' := h 0 [10,-10] 3 (by decide) (by decide) (by decide)
  exact absurd (h' 10 (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_ScanWithModularCarry
