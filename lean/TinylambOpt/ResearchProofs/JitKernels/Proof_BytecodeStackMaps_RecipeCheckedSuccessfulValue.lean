import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# RecipeCheckedSuccessfulValue

Whenever width-checked rematerialization of a recipe succeeds with value `v`,
`v` equals the unbounded evaluation `eval regs r`: the guards in `checked` may
reject a recipe, but they never change its arithmetic result. -/

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps_RecipeCheckedSuccessfulValue
open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

private theorem bind_some {α β : Type} (x : Option α) (f : α → Option β) {v : β}
    (h : (x >>= f) = some v) : ∃ a, x = some a ∧ f a = some v := by
  cases x with
  | none => simp at h
  | some a => exact ⟨a, rfl, h⟩

theorem sound : RecipeCheckedSuccessfulValue.Goal := by
  intro w regs r
  induction r with
  | reg i => exact fun v h => Option.some.inj h.symm
  | lit v => exact fun _ h => Option.some.inj h.symm
  | plus a b iha ihb =>
    intro v h
    have hdef : checked w regs (.plus a b)
        = (checked w regs a).bind fun x => (checked w regs b).bind fun y =>
            if widthOK w (x + y) then some (x + y) else none := rfl
    rw [hdef] at h
    rcases bind_some (checked w regs a) _ h with ⟨x, hxa, hx⟩
    rcases bind_some (checked w regs b) _ hx with ⟨y, hyb, hy⟩
    by_cases hc : widthOK w (x + y) = true
    · rw [if_pos hc] at hy
      obtain rfl : v = x + y := Option.some.inj hy.symm
      show x + y = eval regs a + eval regs b
      rw [iha x hxa, ihb y hyb]
    · rw [if_neg hc] at hy
      exact absurd hy (by simp)
  | neg a ih =>
    intro v h
    have hdef : checked w regs (.neg a)
        = (checked w regs a).bind fun x => if widthOK w (-x) then some (-x) else none := rfl
    rw [hdef] at h
    rcases bind_some (checked w regs a) _ h with ⟨x, hxa, hx⟩
    by_cases hc : widthOK w (-x) = true
    · rw [if_pos hc] at hx
      obtain rfl : v = -x := Option.some.inj hx.symm
      show -x = -eval regs a
      rw [ih x hxa]
    · rw [if_neg hc] at hx
      exact absurd hx (by simp)
