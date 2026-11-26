import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

/-
Refutes `ArithmeticEquivalenceStackMerge`: two recipes with identical
unbounded results can differ in checked results when one overflows an
intermediate operation. The witness overflows a width-2 intermediate but
returns to range.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_ArithmeticEquivalenceStackMerge

open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

theorem refute : ¬ ArithmeticEquivalenceStackMerge.Goal := by
  intro h
  let a : Recipe := .plus (.plus (.lit 3) (.lit 1)) (.neg (.lit 4))
  let b : Recipe := .lit 0
  have heval : ∀ regs, eval regs a = eval regs b := by intro regs; simp [a, b, eval]
  have hc := h a b heval 2 (fun _ => 0)
  have hn : checked 2 (fun _ => 0) a = none := by native_decide
  have hs : checked 2 (fun _ => 0) b = some 0 := by native_decide
  rw [hn, hs] at hc
  contradiction

end TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_ArithmeticEquivalenceStackMerge
