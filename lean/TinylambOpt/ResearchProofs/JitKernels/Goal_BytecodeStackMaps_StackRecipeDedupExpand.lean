import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_StackRecipeDedupExpand

/-- Erasing duplicate recipes changes only the address used to read a
rematerialized value; expanding through `idxOf` restores one result per logical
stack slot, including repeated slots. -/
theorem sound : TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.StackRecipeDedupExpand.Goal := by
  intro regs stack
  dsimp
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_map, List.getElem?_map]
  cases h : stack[i]? with
  | none => simp [h]
  | some r =>
      have hm : r ∈ stack := by
        rcases List.getElem?_eq_some_iff.mp h with ⟨lt, eq⟩
        rw [← eq]
        exact List.getElem_mem lt
      have hc : r ∈ stack.eraseDups := (List.mem_eraseDups).2 hm
      have hlt : stack.eraseDups.idxOf r < stack.eraseDups.length :=
        List.idxOf_lt_length_of_mem hc
      have hElem : stack.eraseDups[stack.eraseDups.idxOf r] = r :=
        List.getElem_idxOf hlt
      have hOpt : stack.eraseDups[stack.eraseDups.idxOf r]? = some r := by
        rw [List.getElem?_eq_getElem hlt, hElem]
      have heq :
          (List.map (TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.eval regs)
              stack.eraseDups)[stack.eraseDups.idxOf r]? =
            some (TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps.eval regs r) := by
        rw [List.getElem?_map, hOpt]
        rfl
      simp [h]
      exact ⟨r, hOpt, rfl⟩

end TinylambOpt.ResearchProofs.JitKernels.Goal_BytecodeStackMaps_StackRecipeDedupExpand
