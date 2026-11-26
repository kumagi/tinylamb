import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-! A whole checked chunk is safe exactly when its least and greatest prefix
    excursions fit the interval. The compiled kernel can therefore test one
    extrema summary before running unchecked additions on every lane. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_PrefixExcursionSingleGuard

open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.PrefixExcursionSingleGuard.Goal := by
  intro lo hi s xs
  have hminSome : ∀ l : List Int, l ≠ [] → ∃ m, l.min? = some m := by
    intro l hl
    cases l with
    | nil => simp at hl
    | cons x t => rw [List.min?_cons]; exact ⟨_, rfl⟩
  have hmaxSome : ∀ l : List Int, l ≠ [] → ∃ m, l.max? = some m := by
    intro l hl
    cases l with
    | nil => simp at hl
    | cons x t => rw [List.max?_cons]; exact ⟨_, rfl⟩
  have hpath : path xs ≠ [] := by simp [path]
  obtain ⟨mn, hmn⟩ := hminSome (path xs) hpath
  obtain ⟨mx, hmx⟩ := hmaxSome (path xs) hpath
  have hmnProp := (List.min?_eq_some_iff.mp hmn)
  have hmxProp := (List.max?_eq_some_iff.mp hmx)
  have hmnD : (path xs).min?.getD 0 = mn := by simp [hmn]
  have hmxD : (path xs).max?.getD 0 = mx := by simp [hmx]
  rw [low, hmnD, high, hmxD]
  constructor
  · intro hs
    change (path xs).all (fun v => lo ≤ s + v && s + v ≤ hi) = true at hs
    have hall := List.all_eq_true.mp hs
    have hmnSafe := hall mn hmnProp.1
    have hmxSafe := hall mx hmxProp.1
    constructor <;> simp at hmnSafe hmxSafe <;> omega
  · intro hb
    change (path xs).all (fun v => lo ≤ s + v && s + v ≤ hi) = true
    apply List.all_eq_true.mpr
    intro v hv
    have hmnLe : mn ≤ v := hmnProp.2 v hv
    have hleMx : v ≤ mx := hmxProp.2 v hv
    simp
    constructor <;> omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_PrefixExcursionSingleGuard
