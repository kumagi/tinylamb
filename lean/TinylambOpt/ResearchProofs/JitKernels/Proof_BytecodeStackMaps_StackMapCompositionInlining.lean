import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# StackMapCompositionInlining

Composing two tier transitions by substituting rematerialization recipes into a
stack map is the same as re-pointing the register file at the values of those
recipes: `eval regs (subst f r) = eval (fun i => eval regs (f i)) r` for every
recipe, hence the two reconstructions of a logical stack agree entry by entry.
This lets the JIT fuse a deopt-into-deopt chain without materializing the
intermediate frame. -/

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps_StackMapCompositionInlining
open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private theorem eval_subst (regs : Regs) (f : Nat → Recipe) :
    ∀ r : Recipe, eval regs (subst f r) = eval (fun i => eval regs (f i)) r := by
  intro r
  induction r with
  | reg i => rfl
  | lit v => rfl
  | plus a b iha ihb => simp [eval, subst, iha, ihb]
  | neg a ih => simp [eval, subst, ih]

theorem sound : StackMapCompositionInlining.Goal := by
  intro regs f stack
  induction stack with
  | nil => rfl
  | cons r rs ih => simp [List.map_cons, ih, eval_subst regs f r]
