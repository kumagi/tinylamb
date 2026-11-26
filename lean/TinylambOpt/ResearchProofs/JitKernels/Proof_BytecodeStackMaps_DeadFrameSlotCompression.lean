import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# DeadFrameSlotCompression

Gathering demanded logical slots can be done directly through the recipe map:
indexing the evaluated frame `stack.map (eval regs)` at position `i` yields the
evaluation of `stack[i]?`, so filtering the wanted indices against the raw frame
and then rematerializing every hit computes the same list as evaluating first. -/

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps_DeadFrameSlotCompression
open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private theorem filterMap_map {α β γ : Type} (l : List α) (h : α → Option β) (g : β → γ) :
    l.filterMap (fun a => (h a).map g) = (l.filterMap h).map g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    simp only [List.filterMap]
    cases h a with
    | none => simp [ih]
    | some b => simp [ih]

theorem sound : DeadFrameSlotCompression.Goal := by
  intro regs stack wanted
  simp only [List.getElem?_map]
  show wanted.filterMap (fun i => (stack[i]?).map (eval regs))
      = (wanted.filterMap (fun i => stack[i]?)).map (eval regs)
  exact filterMap_map wanted (fun i => stack[i]?) (eval regs)
