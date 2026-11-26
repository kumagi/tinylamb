import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# RootRegisterOnlySpill (refuted)

The claim says agreeing on registers whose *bare* `.reg` recipe sits on the
stack suffices to preserve the reconstructed stack.  It is false: a compound
recipe such as `plus (reg 0) (reg 0)` still reads register 0 even though
`.reg 0` is not itself a stack slot.  Counterexample: the single-slot stack
`[plus (reg 0) (reg 0)]` with register files `a = 0`, `b = 1`: the hypothesis
is vacuous but the reconstructions are `[0]` vs `[2]`. -/

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps_RootRegisterOnlySpill
open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private def A : Regs := fun _ => 0
private def B : Regs := fun _ => 1
private def S : List Recipe := [.plus (.reg 0) (.reg 0)]

theorem refute : ¬ RootRegisterOnlySpill.Goal := by
  intro h
  have hpre : ∀ i, .reg i ∈ S → A i = B i := by
    intro i hi
    exact absurd hi (by simp [S])
  have hne : List.map (eval A) S ≠ List.map (eval B) S := by decide
  exact hne (h A B S hpre)
