import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Refuted: strict arithmetic all-NULL shortcut.** Strictness is a per-lane
property: two operands may each contain a NULL on different lanes while some
lane stays non-null in both. Words `1` and `2` inside a three-row packet each
hit the window, yet lane 2 of `a ||| b` remains valid, so the packet cannot be
bypassed as all-NULL.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_StrictArithmeticAllNull
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem refute : ¬ StrictArithmeticAllNull.Goal := by
  intro h
  have ha : (1 : W) &&& window 3 ≠ 0 := by decide
  have hb : (2 : W) &&& window 3 ≠ 0 := by decide
  have hh := h (1 : W) (2 : W) 3 ha hb
  exact (by decide : ¬ valid 3 ((1 : W) ||| (2 : W)) = 0) hh

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_StrictArithmeticAllNull
