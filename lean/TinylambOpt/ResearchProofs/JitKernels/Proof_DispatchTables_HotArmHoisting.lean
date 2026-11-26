import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! SOUND: Hot-arm hoisting. Moving a frequently taken arm `a` to the front of a CASE ladder
preserves the first-match result for every key, provided every predecessor that also matches
`a`'s range returns the same result code. Then stealing `a`'s matches from those predecessors
changes nothing observable, so the JIT may front-load hot arms without fix-up code. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DispatchTables_HotArmHoisting
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-- First-match decomposition of a ladder at its head arm. -/
private theorem pick_cons {a : Arm} {as : List Arm} {x : Int} :
    pick (a :: as) x = if holds a x then some a.result else pick as x := by
  cases h : holds a x
  · simp [pick, h]
  · simp [pick, h]

/-- Soundness of hot-arm hoisting. -/
theorem sound : HotArmHoisting.Goal := by
  intro a before tail h x
  induction before with
  | nil => rfl
  | cons b bs ih =>
    have hb : ∀ c ∈ bs, ∀ y, holds a y → holds c y → c.result = a.result := by
      intro c hc y h1 h2
      exact h c (List.mem_cons_of_mem b hc) y h1 h2
    simp only [List.cons_append] at ih ⊢
    rw [pick_cons, pick_cons]
    by_cases hbx : holds b x = true
    · -- LHS commits to `b`; if `a` also fires, priorities agree by hypothesis.
      rw [if_pos hbx]
      by_cases hax : holds a x = true
      · rw [if_pos hax]
        exact congrArg some (h b List.mem_cons_self x hax hbx)
      · rw [if_neg hax, pick_cons, if_pos hbx]
    · rw [if_neg hbx, ih hb]
      rw [pick_cons]
      by_cases hax : holds a x = true
      · rw [if_pos hax, if_pos hax]
      · rw [if_neg hax, if_neg hax, pick_cons, if_neg hbx]

end TinylambOpt.ResearchProofs.JitKernels.DispatchTables_HotArmHoisting
