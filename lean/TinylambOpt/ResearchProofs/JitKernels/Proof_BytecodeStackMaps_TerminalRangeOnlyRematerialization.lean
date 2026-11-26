import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# TerminalRangeOnlyRematerialization (refuted)

An in-range final value does not license rematerialization: `checked` also
guards every intermediate operation of the recipe.  Counterexample: recipe
`r = r0 + (r1 + r2)` with width 1 and registers `r0 = 4, r1 = -4, r2 = 0`:
the result `0` is in range, but the intermediate `-4` faults, so `checked`
returns `none` instead of `some 0`. -/

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps_TerminalRangeOnlyRematerialization
open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private def r : Recipe := .plus (.reg 0) (.plus (.reg 1) (.reg 2))
private def s : Regs := fun i => if i == 0 then 4 else if i == 1 then -4 else 0

theorem refute : ¬ TerminalRangeOnlyRematerialization.Goal := by
  intro h
  have hw : widthOK 1 (eval s r) = true := by decide
  have hc : checked 1 s r = none := by decide
  have hcon := h 1 s r hw
  rw [hc] at hcon
  exact absurd hcon (by simp)
