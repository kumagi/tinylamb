import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# SignedZeroCostDoubleNegation (refuted)

Signed widths are asymmetric: `x = -2^w` satisfies the guard `widthOK w x`, but
`-x = 2^w` does not, so `checked` on `neg r` fails even though the double
negation would land back in range and `checked r` itself (a bare register read)
succeeded.  Counterexample: width `1`, register file constantly `-2`, recipe
`.reg 0`: `checked` of the recipe is `some (-2)` while `checked` of
`.neg (.neg (.reg 0))` is `none` (the inner negation faults). -/

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps_SignedZeroCostDoubleNegation
open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private def regs : Regs := fun _ => -2

private theorem ok1 : checked 1 regs (.reg 0) = some (eval regs (.reg 0)) := by decide
private theorem ok2 : widthOK 1 (eval regs (.reg 0)) := by decide
private theorem dbl : checked 1 regs (.neg (.neg (.reg 0))) = none := by decide

theorem refute : ¬ SignedZeroCostDoubleNegation.Goal := by
  intro h
  have hcon := h 1 regs (.reg 0) ok1 ok2
  rw [dbl, ok1] at hcon
  exact absurd hcon (by simp)
