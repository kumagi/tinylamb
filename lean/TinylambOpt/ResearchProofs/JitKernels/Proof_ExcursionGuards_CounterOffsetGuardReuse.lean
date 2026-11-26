import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
Raising the entry accumulator from `a` to `b` (with `a ≤ b`) can only lift
every intermediate value `b + v` above the lower guard, so the lower side is
inherited from the certificate at `a`; the upper side follows from the chunk's
prefix maximum: `b + v ≤ b + high xs ≤ hi`. A guard checked at a smaller
accumulator therefore stays valid when only the upper margin shrinks, provided
the `high`-based re-check passes. SQL execution can reuse one pre-filtered
kernel body across accumulator offsets.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_CounterOffsetGuardReuse
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

private theorem some_max (xs : List Int) : ∃ m, (path xs).max? = some m := by
  cases h : (path xs).max? with
  | some m => exact ⟨m, rfl⟩
  | none => exact absurd (List.max?_eq_none_iff.mp h) (by simp [path])

theorem sound : CounterOffsetGuardReuse.Goal := by
  intro lo hi a b xs hab hall hb
  obtain ⟨M, hM⟩ := some_max xs
  obtain ⟨_, hBnd⟩ := List.max?_eq_some_iff.mp hM
  have hhigh : high xs = M := by rw [high, hM, Option.getD_some]
  rw [hhigh] at hb
  refine List.all_eq_true.mpr (fun v hv => ?_)
  have h1 := List.all_eq_true.mp hall v hv
  have h2 := hBnd v hv
  rw [Bool.and_eq_true_iff] at h1
  obtain ⟨hle, _⟩ := h1
  have ha := of_decide_eq_true hle
  rw [Bool.and_eq_true_iff]
  exact ⟨decide_eq_true (by omega), decide_eq_true (by omega)⟩

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_CounterOffsetGuardReuse
