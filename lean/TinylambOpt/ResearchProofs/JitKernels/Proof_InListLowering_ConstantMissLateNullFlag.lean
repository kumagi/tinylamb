import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
For an exact non-NULL probe `some x` that misses the key list, the answer is
decided by a single list-wide "contains NULL" flag: with `some x ∉ xs`, the
membership branch of `test` falls through, so the result is `none` exactly
when a `NULL` entry exists and `some false` otherwise — matching the SQL
three-valued outcome of `x NOT IN (…)` with unknown propagation.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering_ConstantMissLateNullFlag
open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem sound : ConstantMissLateNullFlag.Goal := by
  intro xs x
  cases xs with
  | nil => intro _; rfl
  | cons y ys =>
    intro h
    rw [show test (y :: ys) (some x) =
          if (y :: ys).contains (some x) then some true
          else if (y :: ys).contains none then none else some false from by
        show (if _ then _ else if (some x).isNone then _ else _) = _
        simp]
    have hc : (y :: ys).contains (some x) = false := by
      rw [List.contains_eq_mem]; exact decide_eq_false h
    rw [hc]
    simp

end TinylambOpt.ResearchProofs.JitKernels.InListLowering_ConstantMissLateNullFlag
