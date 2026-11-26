import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_CommonBitsDetectDisjointness

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

theorem clash_iff_no_common (a b : Entry) : clash a b = true ↔ ¬ ∃ w, accepts a w ∧ accepts b w := by
  constructor
  · intro h
    rintro ⟨w, ha, hb⟩
    simp [clash] at h
    simp [accepts] at ha hb
    have hcommon :
        (a.value &&& a.mask &&& b.mask) = (b.value &&& a.mask &&& b.mask) := by
      ext i
      change (a.value &&& a.mask &&& b.mask).getLsbD i =
        (b.value &&& a.mask &&& b.mask).getLsbD i
      have hai := congrArg (fun v : Word => v.getLsbD i) ha
      have hbi := congrArg (fun v : Word => v.getLsbD i) hb
      simp only [BitVec.getLsbD_and] at hai hbi ⊢
      cases ha_i : a.mask.getLsbD i <;> cases hb_i : b.mask.getLsbD i <;>
        cases hw : w.getLsbD i <;> cases hav : a.value.getLsbD i <;>
        cases hbv : b.value.getLsbD i <;>
        simp [ha_i, hb_i, hw, hav, hbv] at hai hbi ⊢
    have hz : ((a.value ^^^ b.value) &&& a.mask &&& b.mask) = 0 := by
      apply BitVec.eq_of_getLsbD_eq
      intro i hi
      have hc := congrArg (fun v : Word => v.getLsbD i) hcommon
      simp only [BitVec.getLsbD_and, BitVec.getLsbD_xor] at hc ⊢
      cases ha_i : a.mask.getLsbD i <;> cases hb_i : b.mask.getLsbD i <;>
        cases hav : a.value.getLsbD i <;> cases hbv : b.value.getLsbD i <;>
        simp [ha_i, hb_i, hav, hbv] at hc ⊢
    exact h hz
  · intro h
    by_cases hc : clash a b = true
    · exact hc
    · have hz : ((a.value ^^^ b.value) &&& a.mask &&& b.mask) = 0 := by
        simpa [clash] using hc
      let w : Word := (a.value &&& a.mask) ||| (b.value &&& b.mask)
      have ha : accepts a w := by
        simp [accepts, w]
        apply BitVec.eq_of_getLsbD_eq
        intro i hi
        have hzi := congrArg (fun v : Word => v.getLsbD i) hz
        simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor] at hzi ⊢
        cases ha_i : a.mask.getLsbD i <;> cases hb_i : b.mask.getLsbD i <;>
          cases hav : a.value.getLsbD i <;> cases hbv : b.value.getLsbD i <;>
          simp [ha_i, hb_i, hav, hbv] at hzi ⊢
      have hb : accepts b w := by
        simp [accepts, w]
        apply BitVec.eq_of_getLsbD_eq
        intro i hi
        have hzi := congrArg (fun v : Word => v.getLsbD i) hz
        simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor] at hzi ⊢
        cases ha_i : a.mask.getLsbD i <;> cases hb_i : b.mask.getLsbD i <;>
          cases hav : a.value.getLsbD i <;> cases hbv : b.value.getLsbD i <;>
          simp [ha_i, hb_i, hav, hbv] at hzi ⊢
      exact False.elim (h ⟨w, ha, hb⟩)

theorem sound : TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonBitsDetectDisjointness.Goal := clash_iff_no_common

end TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_CommonBitsDetectDisjointness
