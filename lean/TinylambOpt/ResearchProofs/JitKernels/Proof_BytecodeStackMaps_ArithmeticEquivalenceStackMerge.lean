import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# ArithmeticEquivalenceStackMerge (refuted)

Two recipes that evaluate identically under every register file may still have
different intermediate values: `checked` guards every intermediate `plus`/`neg`
result, and reassociation moves which partial sums are guarded.
Counterexample: `a = (r0 + r1) + r2` and `b = r0 + (r1 + r2)` agree everywhere
(associativity), yet with width 1 and registers `r0 = 4, r1 = -4, r2 = 0` the
left-associated recipe checks intermediate `0` (in range, succeeds with `some 0`)
while the right-associated one checks intermediate `-4` (out of range, `none`). -/

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps_ArithmeticEquivalenceStackMerge
open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private def A : Recipe := .plus (.plus (.reg 0) (.reg 1)) (.reg 2)
private def B : Recipe := .plus (.reg 0) (.plus (.reg 1) (.reg 2))
private def s : Regs := fun i => if i == 0 then 4 else if i == 1 then -4 else 0

private theorem hab : ∀ regs, eval regs A = eval regs B := by
  intro regs
  show (regs 0 + regs 1) + regs 2 = regs 0 + (regs 1 + regs 2)
  omega

theorem refute : ¬ ArithmeticEquivalenceStackMerge.Goal := by
  intro h
  have h1 : checked 1 s A = some 0 := by decide
  have h2 : checked 1 s B = none := by decide
  have hcon := h A B hab 1 s
  rw [h1, h2] at hcon
  exact absurd hcon (by simp)
