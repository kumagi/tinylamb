import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! A clash on commonly required bits is exactly the static condition that prevents any word from satisfying two versions; without a clash their requirements are jointly satisfiable. -/
namespace Nov26.Proof_VersionDispatch_CommonBitsDetectDisjointness
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonBitsDetectDisjointness
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonBitsDetectDisjointness
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonBitsDetectDisjointness
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonBitsDetectDisjointness

private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

theorem sound : Goal := by
  intro a b
  simp only [clash, bne_iff_ne]
  constructor
  · intro hc ⟨w, ha, hb⟩
    have hae := (acc_eq a w).mp ha
    have hbe := (acc_eq b w).mp hb
    bv_decide
  · intro hn hzero
    apply hn
    refine ⟨(a.value &&& a.mask) ||| (b.value &&& b.mask), ?_, ?_⟩
    · apply (acc_eq a _).mpr
      bv_decide
    · apply (acc_eq b _).mpr
      bv_decide

end Nov26.Proof_VersionDispatch_CommonBitsDetectDisjointness