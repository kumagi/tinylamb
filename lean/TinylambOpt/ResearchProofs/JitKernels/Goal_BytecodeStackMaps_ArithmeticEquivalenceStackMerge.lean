import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

/-! Unbounded equivalence does not license merging checked recipes: an
    intermediate overflow can make one recipe fail while a literal succeeds. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_ArithmeticEquivalenceStackMerge

open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

theorem refute :
    ¬ TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.ArithmeticEquivalenceStackMerge.Goal := by
  intro h
  let a : Recipe := .plus (.lit 3) (.lit 1)
  let b : Recipe := .lit 4
  have hab : ∀ regs, eval regs a = eval regs b := by
    intro regs
    simp [a, b, eval]
  have hh := h a b hab 2 (fun _ => 0)
  have hne : (checked 2 (fun _ => 0) a ≠ checked 2 (fun _ => 0) b) := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_ArithmeticEquivalenceStackMerge
