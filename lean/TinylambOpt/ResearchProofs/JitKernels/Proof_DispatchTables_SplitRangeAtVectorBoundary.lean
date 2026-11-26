import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! SOUND: Splitting one arm at a vector boundary. Replacing arm `a = [a.lo, a.hi)` by the
adjacent pair `[a.lo, mid)` and `[mid, a.hi)` (same result code, `a.lo ≤ mid ≤ a.hi`) at the
same ladder priority preserves the picked result for every key: the halves exactly tile
`a`'s range, and keys outside `a` are unaffected, so two vector ranges can share one bounds
check. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DispatchTables_SplitRangeAtVectorBoundary
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-- First-match decomposition of a ladder at its head arm. -/
private theorem pick_cons {a : Arm} {as : List Arm} {x : Int} :
    pick (a :: as) x = if holds a x then some a.result else pick as x := by
  cases h : holds a x
  · simp [pick, h]
  · simp [pick, h]

private theorem holds_lo_arm (a : Arm) (mid x : Int) :
    holds { a with lo := mid } x = decide (mid ≤ x ∧ x < a.hi) := rfl

private theorem holds_hi_arm (a : Arm) (mid x : Int) :
    holds { a with hi := mid } x = decide (a.lo ≤ x ∧ x < mid) := rfl

/-- Soundness of splitting an arm at a vector boundary. -/
theorem sound : SplitRangeAtVectorBoundary.Goal := by
  intro before a after mid x hlo hhi
  induction before with
  | nil =>
    simp only [List.cons_append, List.nil_append, pick_cons, holds, decide_eq_true_eq]
    by_cases h1 : a.lo ≤ x <;> by_cases h2 : x < a.hi <;> by_cases h3 : x < mid <;>
      by_cases h4 : mid ≤ x <;>
      simp only [h1, h2, h3, h4, and_true, true_and, and_false, false_and] <;>
      first
      | rfl
      | (exfalso; omega)
  | cons b bs ih =>
    simp only [List.cons_append] at ih ⊢
    rw [pick_cons, pick_cons]
    by_cases hbx : holds b x = true
    · rw [if_pos hbx, if_pos hbx]
    · rw [if_neg hbx, if_neg hbx, ih]

end TinylambOpt.ResearchProofs.JitKernels.DispatchTables_SplitRangeAtVectorBoundary
