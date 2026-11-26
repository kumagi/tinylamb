import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! Two guards clash on a common required bit exactly when no 64-bit word can satisfy both; the converse builds a word from both required values. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_CommonBitsDetectDisjointness

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonBitsDetectDisjointness

theorem clash_iff_no_common (a b : Entry) : clash a b = true ↔ ¬ ∃ w, accepts a w ∧ accepts b w := by
  constructor
  · intro h
    rintro ⟨w, ha, hb⟩
    simp [clash] at h
    simp [accepts] at ha hb
    bv_decide
  · intro h
    by_cases hc : clash a b = true
    · exact hc
    · have hc0 : ((a.value ^^^ b.value) &&& a.mask &&& b.mask) = 0 := by
        simp [clash] at hc
        bv_decide
      let w := (a.value &&& a.mask) ||| (b.value &&& b.mask)
      have ha : accepts a w := by
        simp [accepts, w]
        bv_decide
      have hb : accepts b w := by
        simp [accepts, w]
        bv_decide
      exact False.elim (h ⟨w, ha, hb⟩)

theorem sound : CommonBitsDetectDisjointness.Goal := clash_iff_no_common

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_CommonBitsDetectDisjointness
