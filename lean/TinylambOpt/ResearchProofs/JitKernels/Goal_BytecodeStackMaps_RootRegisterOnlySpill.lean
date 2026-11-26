import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

/-
Refutes `RootRegisterOnlySpill`: registers nested inside a top-level
expression are not listed by `∈ stack`, so spilling only root registers can
change the rematerialized value. The witness puts a register under `plus`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_RootRegisterOnlySpill

open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

theorem refute : ¬ RootRegisterOnlySpill.Goal := by
  intro h
  let a : Regs := fun _ => 1
  let b : Regs := fun _ => 2
  let stack : List Recipe := [.plus (.reg 0) (.lit 0)]
  have hp : ∀ i, (Recipe.reg i) ∈ stack → a i = b i := by
    intro i hi
    simp [stack] at hi
  have heq := h a b stack hp
  simp [eval, a, b, stack] at heq


end TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_RootRegisterOnlySpill
