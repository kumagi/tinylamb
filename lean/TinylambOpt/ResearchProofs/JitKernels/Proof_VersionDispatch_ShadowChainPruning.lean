import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! An earlier weaker guard accepts every row its dominated later guard accepts, so the later entry can never win dispatch and may be removed. -/
namespace Nov26.Proof_VersionDispatch_ShadowChainPruning
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ShadowChainPruning
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ShadowChainPruning
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ShadowChainPruning
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.ShadowChainPruning

private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

theorem sound : Goal := by
  intro a b rest tail w hw
  have himp : ∀ w, accepts b w = true → accepts a w = true := by
    intro w hb
    have hw' := hw
    simp only [weaker, Bool.and_eq_true] at hw'
    have hm : a.mask &&& b.mask = a.mask := by simpa using hw'.1
    have hnc : ((a.value ^^^ b.value) &&& a.mask &&& b.mask) = 0 := by simpa [clash] using hw'.2
    have hbe := (acc_eq b w).mp hb
    clear hw hw'
    apply (acc_eq a w).mpr
    bv_decide
  by_cases ha : accepts a w = true
  · simp [dispatch, ha]
  · have hbnot : ¬ accepts b w = true := by
      intro hb
      exact ha (himp w hb)
    have htail : (b :: tail).find? (accepts · w) = tail.find? (accepts · w) := by
      simp [hbnot]
    simp [dispatch, ha, htail]

end Nov26.Proof_VersionDispatch_ShadowChainPruning