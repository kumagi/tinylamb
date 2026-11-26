import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_DeadFrameSlotCompression

open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.DeadFrameSlotCompression.Goal := by
  intro regs stack wanted
  have hget : ∀ (j : Nat), (stack.map (eval regs))[j]? = Option.map (eval regs) stack[j]? := by
    intro j
    simp
  induction wanted with
  | nil => rfl
  | cons i wanted ih =>
      cases hi : stack[i]? with
      | none => simpa [hi, hget] using ih
      | some r => simpa [hi, hget] using congrArg (List.cons (eval regs r)) ih

end TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_DeadFrameSlotCompression
