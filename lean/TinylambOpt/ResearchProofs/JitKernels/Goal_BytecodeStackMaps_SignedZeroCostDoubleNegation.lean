import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

/-!
Signed widths are asymmetric, so eliminating a double negation is unsound at the
minimum representable value: the inner negation faults even though the original
recipe and the double-negated result are in range. SQL execution must keep the
intermediate guard, otherwise a side exit would silently discard an overflow.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_SignedZeroCostDoubleNegation

open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private def regs : Regs := fun _ => -2

private theorem ok1 : checked 1 regs (.reg 0) = some (eval regs (.reg 0)) := by decide
private theorem ok2 : widthOK 1 (eval regs (.reg 0)) := by decide
private theorem dbl : checked 1 regs (.neg (.neg (.reg 0))) = none := by decide

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.SignedZeroCostDoubleNegation.Goal := by
  intro h
  have hcon := h 1 regs (.reg 0) ok1 ok2
  rw [dbl, ok1] at hcon
  exact absurd hcon (by simp)

end TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_SignedZeroCostDoubleNegation
