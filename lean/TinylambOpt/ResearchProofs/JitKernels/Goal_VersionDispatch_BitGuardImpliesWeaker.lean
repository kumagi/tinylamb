import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-!
A guard is weaker exactly when every word accepted by the stronger guard is
also accepted by it. The forward direction is a bitwise argument; the converse
builds a distinguishing word whose bits are forced by the acceptance hypothesis.
SQL dispatch may therefore drop a dominated guard without changing the chosen
code for any feature word.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_BitGuardImpliesWeaker

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

theorem weaker_imp (a b : Entry) (h : weaker a b = true) : ∀ w, accepts b w → accepts a w := by
  simp [weaker, clash] at h
  intro w hw
  simp [accepts] at hw ⊢
  apply BitVec.eq_of_getElem_eq
  intro i hi
  have hm := congrArg (fun z : Word => z.getLsbD i) h.1
  have hc := congrArg (fun z : Word => z.getLsbD i) h.2
  have hwbits := congrArg (fun z : Word => z.getLsbD i) hw
  have hm' := by
    simpa only [BitVec.getLsbD_eq_getElem hi, BitVec.getElem_and hi] using hm
  have hc' := by
    simpa only [BitVec.getLsbD_eq_getElem hi, BitVec.getElem_and hi, BitVec.getElem_xor hi,
      BitVec.getElem_zero hi] using hc
  have hwbits' := by
    simpa only [BitVec.getLsbD_eq_getElem hi, BitVec.getElem_and hi] using hwbits
  rw [BitVec.getElem_and hi, BitVec.getElem_and hi]
  cases ha : a.mask[i] <;> cases hbm : b.mask[i] <;>
  cases hwb : w[i] <;> cases hav : a.value[i] <;> cases hbv : b.value[i] <;>
  simp_all

theorem sound : BitGuardImpliesWeaker.Goal := by
  intro a b
  constructor
  · exact weaker_imp a b
  · intro h
    let w1 := (b.value &&& b.mask) ||| (a.mask &&& ~~~b.mask &&& ~~~a.value)
    have hb1 : accepts b w1 := by
      simp [accepts]
      apply BitVec.eq_of_getElem_eq
      intro i hi
      have hlsb : (w1 &&& b.mask).getLsbD i = (b.value &&& b.mask).getLsbD i := by
        cases hbm : b.mask[i] <;> cases hbv : b.value[i] <;>
          cases ham : a.mask[i] <;> cases hav : a.value[i] <;>
          simp [w1, hbm, hbv, ham, hav, hi, BitVec.getLsbD_eq_getElem hi, BitVec.getLsbD_and,
            BitVec.getLsbD_or, BitVec.getLsbD_not]
      simpa [BitVec.getLsbD_eq_getElem hi, BitVec.getElem_and hi] using hlsb
    have ha1 := h w1 hb1
    simp [accepts] at ha1
    have hab : ∀ i, i < 64 → a.mask.getLsbD i = true → b.mask.getLsbD i = false → False := by
      intro i hi ham hbm
      have hbit := congrArg (fun z : Word => z.getLsbD i) ha1
      simp [BitVec.getLsbD_and] at hbit
      cases hbv : b.value.getLsbD i <;> cases hav : a.value.getLsbD i <;>
        simp [w1, hbm, hbv, ham, hav, hi, BitVec.getLsbD_and, BitVec.getLsbD_or,
          BitVec.getLsbD_not] at hbit <;>
        simp_all
    have hval : ∀ i, i < 64 → a.mask.getLsbD i = true → b.mask.getLsbD i = true →
        a.value.getLsbD i = b.value.getLsbD i := by
      intro i hi ham hbm
      have hbit := congrArg (fun z : Word => z.getLsbD i) ha1
      simp [BitVec.getLsbD_and] at hbit
      cases hav : a.value.getLsbD i <;> cases hbv : b.value.getLsbD i <;>
        simp [w1, hbm, hbv, ham, hav, hi, BitVec.getLsbD_and, BitVec.getLsbD_or,
          BitVec.getLsbD_not] at hbit <;>
        simp_all
    simp [weaker, clash]
    constructor
    · apply BitVec.eq_of_getElem_eq
      intro i hi
      have hlsb : (a.mask &&& b.mask).getLsbD i = a.mask.getLsbD i := by
        cases ham : a.mask.getLsbD i <;> cases hbm : b.mask.getLsbD i <;>
          simp [ham, hbm, BitVec.getLsbD_and]
        · exfalso
          exact hab i hi ham hbm
      simpa [BitVec.getLsbD_eq_getElem hi, BitVec.getElem_and hi] using hlsb
    · apply BitVec.eq_of_getElem_eq
      intro i hi
      have hlsb : ((a.value ^^^ b.value) &&& a.mask &&& b.mask).getLsbD i = false := by
        cases hav : a.value.getLsbD i <;> cases hbv : b.value.getLsbD i <;>
          cases ham : a.mask.getLsbD i <;> cases hbm : b.mask.getLsbD i <;>
          simp [hav, hbv, ham, hbm, BitVec.getLsbD_and, BitVec.getLsbD_xor]
        all_goals
          exfalso
          exact (by simpa [hav, hbv] using (hval i hi ham hbm))
      simpa [BitVec.getLsbD_eq_getElem hi, BitVec.getElem_and hi, BitVec.getElem_xor hi,
        BitVec.getElem_zero hi] using hlsb

end TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_BitGuardImpliesWeaker
