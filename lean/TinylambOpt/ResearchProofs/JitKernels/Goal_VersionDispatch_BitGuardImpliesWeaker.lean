import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! A guard is weaker exactly when every word accepted by the stronger guard is also accepted by it; the proof constructs explicit distinguishing words for the converse. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_BitGuardImpliesWeaker

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.BitGuardImpliesWeaker

theorem weaker_imp (a b : Entry) (h : weaker a b = true) : ∀ w, accepts b w → accepts a w := by
  intro w
  simp [weaker, accepts, clash] at h ⊢
  bv_decide

theorem sound : BitGuardImpliesWeaker.Goal := by
  intro a b
  constructor
  · exact weaker_imp a b
  · intro h
    let w1 := (b.value &&& b.mask) ||| (a.mask &&& ~~~b.mask &&& ~~~a.value)
    have hb1 : accepts b w1 := by
      simp [accepts, w1]
      bv_decide
    have ha1 : accepts a w1 := h w1 hb1
    have hsub : a.mask &&& b.mask = a.mask := by
      simp [accepts, w1] at ha1
      bv_decide
    let w2 := b.value &&& b.mask
    have hb2 : accepts b w2 := by
      simp [accepts, w2]
      bv_decide
    have ha2 : accepts a w2 := h w2 hb2
    have hnc : ((a.value ^^^ b.value) &&& a.mask &&& b.mask) = 0 := by
      simp [accepts, w2] at ha2
      bv_decide
    simp [weaker, clash]
    bv_decide

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_BitGuardImpliesWeaker
