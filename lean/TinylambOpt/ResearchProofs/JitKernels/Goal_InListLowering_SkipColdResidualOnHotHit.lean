import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-! A positive result from the hot range-filtered IN test short-circuits the
    cold residual: the same probe is present in the original key list, so the
    unfiltered three-valued test also accepts it. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_SkipColdResidualOnHotHit

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem sound : TinylambOpt.ResearchGoals.JitKernels.InListLowering.SkipColdResidualOnHotHit.Goal := by
  intro xs lo hi v hk
  let ys : Keys := rangeKeys xs lo hi
  have htest : test ys v = some true := by
    simpa [keep, ys] using hk
  have hysne : ys ≠ [] := by
    intro hnil
    rw [hnil] at htest
    simp [test] at htest
  have hysEmpty : ys.isEmpty = false := by
    cases h : ys.isEmpty
    · rfl
    · exfalso
      exact hysne (List.isEmpty_iff.mp h)
  have hvnotNone : v ≠ none := by
    intro hvnone
    rw [hvnone] at htest
    simp [test, hysEmpty] at htest
  have hmemHot : v ∈ ys := by
    unfold test at htest
    simp [hysEmpty, hvnotNone] at htest
    exact htest
  have hmem : v ∈ xs := (List.mem_filter.mp (by simpa [ys, rangeKeys] using hmemHot)).1
  have hxsEmpty : xs.isEmpty = false := by
    cases h : xs.isEmpty
    · rfl
    · exfalso
      have hnil : xs = [] := List.isEmpty_iff.mp h
      rw [hnil] at hmem
      simp at hmem
  unfold test
  simp [hxsEmpty, hvnotNone, List.elem_eq_mem, hmem]

end TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_SkipColdResidualOnHotHit
