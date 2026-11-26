import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! SOUND: Deleting a shadowed arm. If arm `b` appears later in the ladder than arm `a` and
`a`'s half-open range contains `b`'s (`a.lo ≤ b.lo`, `b.hi ≤ a.hi`), then removing `b`
preserves the ladder result for every key even when `b.result ≠ a.result`: any key reaching
`b` would already have matched `a`, so `b`'s code was unreachable. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DispatchTables_ShadowedArmDeletion
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-- First-match decomposition of a ladder at its head arm. -/
private theorem pick_cons {a : Arm} {as : List Arm} {x : Int} :
    pick (a :: as) x = if holds a x then some a.result else pick as x := by
  cases h : holds a x
  · simp [pick, h]
  · simp [pick, h]

/-- A key matching the shadowed arm `b` also matches the containing earlier arm `a`. -/
private theorem shadow_holds (a b : Arm) (hlo : a.lo ≤ b.lo) (hhi : b.hi ≤ a.hi) (x : Int)
    (hb : holds b x) : holds a x := by
  simp only [holds] at hb ⊢
  obtain ⟨h1, h2⟩ := of_decide_eq_true hb
  exact decide_eq_true (And.intro (by omega) (by omega))

/-- Soundness of shadowed-arm deletion. -/
theorem sound : ShadowedArmDeletion.Goal := by
  intro a b middle tail x hlo hhi
  simp only [List.cons_append]
  rw [pick_cons, pick_cons]
  by_cases ha : holds a x = true
  · rw [if_pos ha, if_pos ha]
  · rw [if_neg ha, if_neg ha]
    induction middle with
    | nil =>
      simp only [List.nil_append]
      rw [pick_cons]
      by_cases hb : holds b x = true
      · rw [if_pos hb]
        exact absurd (shadow_holds a b hlo hhi x hb) ha
      · rw [if_neg hb]
    | cons m ms ih =>
      simp only [List.cons_append] at ih ⊢
      rw [pick_cons, pick_cons]
      by_cases hm : holds m x = true
      · rw [if_pos hm, if_pos hm]
      · rw [if_neg hm, if_neg hm, ih]

end TinylambOpt.ResearchProofs.JitKernels.DispatchTables_ShadowedArmDeletion
