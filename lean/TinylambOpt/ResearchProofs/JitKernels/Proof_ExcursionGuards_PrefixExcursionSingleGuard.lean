import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
A chunk's unchecked additions all lie in `[lo - s, hi - s]` exactly when the
chunk's prefix-minimum and prefix-maximum do: membership acceptance of the
value range reduces to its two extremes because `fun v => lo ≤ s+v && s+v ≤ hi`
is monotone in `v`. The proof obtains the actual min/max witnesses from
`min?_eq_some_iff`/`max?_eq_some_iff` (the scan lattice `path xs` starts with
`0`, so it is never empty and `getD` never fires) and pushes the range check
through the order. SQL execution may therefore gate a whole vectorized chunk
by two scalars.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_PrefixExcursionSingleGuard
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

private theorem some_min (xs : List Int) : ∃ m, (path xs).min? = some m := by
  cases h : (path xs).min? with
  | some m => exact ⟨m, rfl⟩
  | none => exact absurd (List.min?_eq_none_iff.mp h) (by simp [path])

private theorem some_max (xs : List Int) : ∃ m, (path xs).max? = some m := by
  cases h : (path xs).max? with
  | some m => exact ⟨m, rfl⟩
  | none => exact absurd (List.max?_eq_none_iff.mp h) (by simp [path])

theorem sound : PrefixExcursionSingleGuard.Goal := by
  intro lo hi s xs
  obtain ⟨m, hm⟩ := some_min xs
  obtain ⟨M, hM⟩ := some_max xs
  obtain ⟨hmem, hbnd⟩ := List.min?_eq_some_iff.mp hm
  obtain ⟨hMem, hBnd⟩ := List.max?_eq_some_iff.mp hM
  have hlow : low xs = m := by rw [low, hm, Option.getD_some]
  have hhigh : high xs = M := by rw [high, hM, Option.getD_some]
  constructor
  · intro hall
    have h1 := List.all_eq_true.mp hall m hmem
    have h2 := List.all_eq_true.mp hall M hMem
    rw [Bool.and_eq_true_iff] at h1 h2
    obtain ⟨hle, _⟩ := h1
    obtain ⟨_, hup⟩ := h2
    rw [hlow, hhigh]
    exact ⟨of_decide_eq_true hle, of_decide_eq_true hup⟩
  · intro ⟨hlo, hhi⟩
    rw [hlow] at hlo
    rw [hhigh] at hhi
    refine List.all_eq_true.mpr (fun v hv => ?_)
    have h1 := hbnd v hv
    have h2 := hBnd v hv
    rw [Bool.and_eq_true_iff]
    exact ⟨decide_eq_true (by omega), decide_eq_true (by omega)⟩

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_PrefixExcursionSingleGuard
