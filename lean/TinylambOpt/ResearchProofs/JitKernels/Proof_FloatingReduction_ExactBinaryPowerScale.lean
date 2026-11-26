import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
import TinylambOpt.ResearchProofs.JitKernels.Common_FloatingReduction

/-!
Proves `ExactBinaryPowerScale`: for finite binary64 operands with magnitude at
least one whose doublings stay finite, distributing the factor of two into the
summand additions preserves every result bit. The proof works in the IEEE model:
inputs are unpacked into normalized finite mantissas, `add_succ` relates the
scaled addition to scaling the unscaled sum, and canonical pack/unpack facts
bridge the model back to observable `Float.toBits`.
-/

namespace Nov26.Proof_FloatingReduction_ExactBinaryPowerScale

open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionCommon
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open Float.Model
open Float.Model.UnpackedFloat
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

private theorem unpack_finite_bounds {x : Float.Model} {s : Sign} {m : Nat} {e : Int} {hm : 0 < m}
    (h : x.unpack = .finite s m e hm) :
    (-1074 : Int) ≤ e ∧ e ≤ 971 ∧ m < 2^53 ∧ (-1074 < e → 2^52 ≤ m) := by

  let ev : BitVec 11 := unpackExponent (spec := Format.binary64) x.toBits.toBitVec
  let mv : BitVec 52 := unpackMantissa (spec := Format.binary64) x.toBits.toBitVec
  have h' := h
  dsimp [Float.Model.unpack, UnpackedFloat.unpack, ev, mv] at h'
  have hbias : Format.binary64.exponentBias = 1023 := by decide
  have hpow : (2:Nat)^52 = 4503599627370496 := by decide
  by_cases hone : ev = 2047#11
  · have hone' : unpackExponent (spec := Format.binary64) x.toBits.toBitVec = 2047#11 := by simpa [ev] using hone
    by_cases hmz : unpackMantissa (spec := Format.binary64) x.toBits.toBitVec = 0#52
    · simp [hone', hmz] at h'
    · simp [hone', hmz] at h'
  · have hone' : unpackExponent (spec := Format.binary64) x.toBits.toBitVec ≠ 2047#11 := by simpa [ev] using hone
    by_cases hzero : ev = 0#11
    · have hzero' : unpackExponent (spec := Format.binary64) x.toBits.toBitVec = 0#11 := by simpa [ev] using hzero
      by_cases hmz : unpackMantissa (spec := Format.binary64) x.toBits.toBitVec = 0#52
      · simp [hone', hzero', hmz] at h'
      · simp [hone', hzero', hmz] at h'
        have hmval : m = mv.toNat := by
          have := h'.2.1
          simpa [mv] using this.symm
        have heval : e = -((Format.binary64.exponentBias : Int) + 52) + 1 := by
          exact h'.2.2.symm
        rw [hmval, heval, hbias]
        have hmv : mv.toNat < 2^52 := mv.isLt
        constructor; omega
        constructor; omega
        constructor
        · exact Nat.lt_trans hmv (by decide)
        · intro he; omega
    · have hzero' : unpackExponent (spec := Format.binary64) x.toBits.toBitVec ≠ 0#11 := by simpa [ev] using hzero
      simp [hone', hzero'] at h'
      have hmval : m = 4503599627370496 ||| mv.toNat := by
        simpa [mv] using h'.2.1.symm
      have heval : e = (ev.toNat : Int) - ((Format.binary64.exponentBias : Int) + 52) := by
        simpa [ev] using h'.2.2.symm
      have hepos : 0 < ev.toNat := by
        by_cases hz : ev.toNat = 0
        · have : ev = 0#11 := BitVec.eq_of_toNat_eq (by simpa using hz)
          exact (hzero this).elim
        · omega
      have helt : ev.toNat < 2048 := ev.isLt
      have he2047 : ev.toNat ≠ 2047 := by
        intro ht
        apply hone
        apply BitVec.eq_of_toNat_eq
        have h2047 : (2047#11).toNat = 2047 := by decide
        rw [h2047]
        exact ht
      have heup : ev.toNat ≤ 2046 := by omega
      have hmvlt : mv.toNat < 2^52 := mv.isLt
      have hor : 4503599627370496 ||| mv.toNat = 2^52 + mv.toNat := by
        rw [← hpow, Nat.or_comm]
        simpa [Nat.add_comm] using Nat.or_two_pow_eq_add_of_lt hmvlt
      rw [hmval, heval, hbias, hor]
      have hpowpos : 0 < (2:Nat)^52 := by decide
      have hpow53 : (2:Nat)^53 = 2 * (2:Nat)^52 := by decide
      rw [hpow53]
      constructor
      · omega
      constructor
      · omega
      constructor <;> omega


private theorem unpack_pack_finite_canonical (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (hm_lt : m < 2^53) (he_ge : (-1074 : Int) ≤ e) (he_le : e ≤ 971)
    (hcanon : 2^52 ≤ m ∨ e = -1074) :
    (Float.Model.pack (.finite s m e hm)).unpack = .finite s m e hm := by

  have hbias : Format.binary64.exponentBias = 1023 := by decide
  have hmb : Format.binary64.mantissaBits = 53 := by decide
  have hpow : (2:Nat)^52 = 4503599627370496 := by decide
  by_cases hnorm : 2^52 ≤ m
  · have hne : m ≠ 0 := by omega
    have hlog : m.log2 = 52 := (Nat.log2_eq_iff hne).mpr ⟨hnorm, hm_lt⟩
    let B := ((e + (Format.binary64.exponentBias : Int) + 52).toNat)
    have hBint : (B : Int) = e + 1075 := by
      dsimp [B]
      rw [hbias]
      change (((e + (1023 : Int) + 52).toNat : Nat) : Int) = e + 1075
      rw [Int.toNat_of_nonneg (by omega : 0 ≤ e + (1023 : Int) + 52)]
      omega
    have hBlt : B < 2048 := by omega
    have hBge : 1 ≤ B := by omega
    have hnotov : ¬ 2047 ≤ B := by omega
    have hmod : m % 2^52 = m - 2^52 := by
      rw [Nat.mod_eq_sub_mod hnorm]
      exact Nat.mod_eq_of_lt (by omega)
    have hmant : (4503599627370496 ||| (m % 2^52)) = m := by
      rw [← hpow, Nat.or_comm]
      rw [Nat.or_two_pow_eq_add_of_lt (by omega : m % 2^52 < (2:Nat)^52)]
      rw [hmod]
      omega
    have hmant' : (4503599627370496 ||| (m - 4503599627370496)) = m := by
      rw [← hpow, Nat.or_comm]
      rw [Nat.or_two_pow_eq_add_of_lt (by omega : m - 2^52 < (2:Nat)^52)]
      omega
    have hexp : (B : Int) - ((Format.binary64.exponentBias : Int) + 52) = e := by
      rw [hBint, hbias]
      omega
    have hexp' : max (e + (Format.binary64.exponentBias : Int) + 52) 0 -
        ((Format.binary64.exponentBias : Int) + 52) = e := by
      rw [hbias]
      change max (e + (1023 : Int) + 52) 0 - ((1023 : Int) + 52) = e
      rw [Int.max_eq_left (by omega : 0 ≤ e + (1023 : Int) + 52)]
      omega
    have hsign : Sign.ofBitVec
        (unpackSign (spec := Format.binary64) (s.toBitVec ++ BitVec.ofNat 11 (e + (Format.binary64.exponentBias : Int) + 52).toNat ++ BitVec.ofNat 52 m)) = s := by
      simpa [packComponents, B, sign_of_toBitVec] using
        congrArg Sign.ofBitVec (unpackSign_pc64 s (BitVec.ofNat 11 B) (BitVec.ofNat 52 m))
    have hev_ne_all : BitVec.ofNat 11 B ≠ 2047#11 := by
      intro hh
      have ht := congrArg BitVec.toNat hh
      simp [BitVec.toNat_ofNat, Nat.mod_eq_of_lt hBlt] at ht
      omega
    have hev_ne_zero : BitVec.ofNat 11 B ≠ 0#11 := by
      intro hh
      have ht := congrArg BitVec.toNat hh
      simp [BitVec.toNat_ofNat, Nat.mod_eq_of_lt hBlt] at ht
      omega
    simp [Float.Model.pack, Float.Model.unpack, UnpackedFloat.pack, UnpackedFloat.unpack,
      hnotov, hev_ne_all, hev_ne_zero, hlog, hmb, B, hBlt, hmant, hmant', hmod, hexp, hexp', hsign,
      packComponents, unpackSign_pc64, unpackExp_append, unpackMan_append,
      sign_of_toBitVec, BitVec.toNat_ofNat, Nat.mod_eq_of_lt hBlt]
  · have hsub : e = -1074 := by
      rcases hcanon with h | h
      · exact (hnorm h).elim
      · exact h
    have hlogne : m.log2 + 1 ≠ Format.binary64.mantissaBits := by
      rw [hmb]
      have hloglt : m.log2 < 52 := (Nat.log2_lt (by omega : m ≠ 0)).mpr (by omega)
      omega
    let B := ((e + (Format.binary64.exponentBias : Int) + 52).toNat)
    have hB : B = 1 := by
      dsimp [B]
      rw [hsub, hbias]
      decide
    have hnotov : ¬ 2 ^ Format.binary64.exponentBits ≤ B + 1 := by
      rw [hB]
      have heb : Format.binary64.exponentBits = 11 := by decide
      rw [heb]
      decide
    have hmod : m % 2^52 = m := Nat.mod_eq_of_lt (by omega)
    have hmvne : BitVec.ofNat 52 m ≠ 0#52 := by
      intro hh
      have ht := congrArg BitVec.toNat hh
      simp [BitVec.toNat_ofNat, hmod] at ht
      omega
    have hsign : Sign.ofBitVec (unpackSign (spec := Format.binary64) (s.toBitVec ++ 0#11 ++ BitVec.ofNat 52 m)) = s := by
      simpa [packComponents, sign_of_toBitVec] using
        congrArg Sign.ofBitVec (unpackSign_pc64 s (0#11) (BitVec.ofNat 52 m))
    have hnotov' : ¬ 2047 ≤ (-1074 + (Format.binary64.exponentBias : Int) + 52).toNat := by
      rw [hbias]
      decide
    simp [Float.Model.pack, Float.Model.unpack, UnpackedFloat.pack, UnpackedFloat.unpack,
      hnotov', hlogne, hsub, hbias, hmod, hmvne, hsign, packComponents, unpackSign_pc64,
      unpackExp_append, unpackMan_append, sign_of_toBitVec, BitVec.toNat_ofNat]


private theorem pack_overflow_exp {s : Sign} {m : Nat} {e : Int} {hm : 0 < m}
    (he : (972 : Int) ≤ e) : (Float.Model.pack (.finite s m e hm)).unpack = .infinity s := by

  have hbias : Format.binary64.exponentBias = 1023 := by decide
  have hmb : Format.binary64.mantissaBitsWithoutImplicit = 52 := by decide
  have heb : Format.binary64.exponentBits = 11 := by decide
  have hnonneg : (0 : Int) ≤ e + 1023 + 52 := by omega
  have hcast : ((e + 1023 + 52).toNat : Int) = e + 1023 + 52 := Int.toNat_of_nonneg hnonneg
  have hcond : 2047 ≤ (e + 1023 + 52).toNat := by omega
  have hpack : UnpackedFloat.pack Format.binary64 (.finite s m e hm) =
      packedInfinity Format.binary64 s := by
    simp [UnpackedFloat.pack, hbias, hmb, heb, hcond]
  simpa [Float.Model.pack, UnpackedFloat.pack, hpack, packedInfinity, hbias, hmb, heb, hcond] using unpack_pack_inf s


private def scaleUF : UnpackedFloat → UnpackedFloat
  | .finite s m e hm => .finite s m (e + 1) hm
  | .zero s => .zero s
  | .infinity s => .infinity s
  | .notANumber => .notANumber

private theorem target_succ_ge (m : Nat) (e : Int) (he : (-104 : Int) ≤ e) :
    Format.binary64.targetExponent (totalExponent m (e+1)) =
      Format.binary64.targetExponent (totalExponent m e) + 1 := by
  have hlog : 0 ≤ (m.log2 : Int) := by omega
  simp [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits,
    Format.exponentBits]
  omega

private theorem shift_exp_ge (m : Nat) (e : Int) (he : (-52 : Int) ≤ e) :
    let te := Format.binary64.targetExponent (totalExponent m e)
    (-104 : Int) ≤ e + (te - e).toNat := by
  dsimp
  let te := Format.binary64.targetExponent (totalExponent m e)
  have hte : (-104 : Int) ≤ te := by
    dsimp [te]
    simp [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits,
      Format.exponentBits]
    omega
  by_cases hle : e ≤ te
  · have hcast : ((te - e).toNat : Int) = te - e := Int.toNat_of_nonneg (by omega)
    omega
  · have hcast : (te - e).toNat = 0 := Int.toNat_of_nonpos (by omega)
    simp [hcast]
    omega

private theorem rwa_succ (s : Sign) (m : Nat) (e : Int) (he : (-104 : Int) ≤ e) :
    roundWithAccuracy Format.binary64 s m (e+1) Accuracy.exact =
      scaleUF (roundWithAccuracy Format.binary64 s m e Accuracy.exact) := by
  let te := Format.binary64.targetExponent (totalExponent m e)
  have hteU : Format.binary64.targetExponent (totalExponent m e) = te := rfl
  have hteScaled : Format.binary64.targetExponent (totalExponent m (e+1)) = te + 1 := by
    dsimp [te]
    exact target_succ_ge m e (by omega : (-104 : Int) ≤ e)
  let sh := (te - e).toNat
  let em1 := ExtendedMantissa.ofMantissaAndAccuracy m Accuracy.exact >>> sh
  let ee := e + sh
  have hee : (-104 : Int) ≤ ee := by
    dsimp [ee, sh]
    by_cases hle : e ≤ te
    · have hcast : ((te - e).toNat : Int) = te - e := Int.toNat_of_nonneg (by omega)
      omega
    · have hcast : (te - e).toNat = 0 := Int.toNat_of_nonpos (by omega)
      simp [hcast]
      omega
  have hshiftSInt : te + 1 - (e + 1) = te - e := by omega
  have hshiftS : shiftToExponent m (e+1) Accuracy.exact (te+1) = (em1, ee+1) := by
    dsimp [shiftToExponent, em1, ee, sh]
    rw [hshiftSInt]
    apply Prod.ext
    · rfl
    · omega
  have hshiftU : shiftToExponent m e Accuracy.exact te = (em1, ee) := by
    rfl
  let r := em1.roundedMantissa
  let te2 := Format.binary64.targetExponent (totalExponent r ee)
  have hte2U : Format.binary64.targetExponent (totalExponent r ee) = te2 := rfl
  have hte2Scaled : Format.binary64.targetExponent (totalExponent r (ee+1)) = te2 + 1 := by
    dsimp [te2]
    exact target_succ_ge r ee hee
  let sh2 := (te2 - ee).toNat
  let em2 := ExtendedMantissa.ofMantissaAndAccuracy r Accuracy.exact >>> sh2
  let ee2 := ee + sh2
  have hshift2SInt : te2 + 1 - (ee + 1) = te2 - ee := by omega
  have hshift2S : shiftToExponent r (ee+1) Accuracy.exact (te2+1) = (em2, ee2+1) := by
    dsimp [shiftToExponent, em2, ee2, sh2]
    rw [hshift2SInt]
    apply Prod.ext
    · rfl
    · omega
  have hshift2U : shiftToExponent r ee Accuracy.exact te2 = (em2, ee2) := by
    rfl
  simp [roundWithAccuracy, shiftToTargetExponent, hteU, hteScaled, hshiftS, hshiftU,
    show em1.roundedMantissa = r by rfl, hte2U, hte2Scaled, hshift2S, hshift2U]
  by_cases hz : em2.mantissa = 0 <;> simp [hz, scaleUF]


private theorem em_shift_zero (em : ExtendedMantissa) : em >>> 0 = em := by rfl

private theorem log2_mul_two_pow (m n : Nat) (hm : m ≠ 0) :
    (m * 2^n).log2 = m.log2 + n := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [Nat.pow_succ]
      rw [← Nat.mul_assoc]
      rw [Nat.mul_comm (m * 2^n) 2]
      rw [Nat.log2_two_mul (Nat.mul_ne_zero hm (Nat.ne_of_gt (Nat.two_pow_pos n))), ih]
      omega

private theorem em_shift_pow_gen (m n k : Nat) :
    ({ mantissa := m * 2^(n+k), roundBit := false, stickyBit := false } : ExtendedMantissa) >>> n =
      { mantissa := m * 2^k, roundBit := false, stickyBit := false } := by
  induction n generalizing k with
  | zero => simp [HShiftRight.hShiftRight, Nat.repeat]
  | succ n ih =>
      simp only [HShiftRight.hShiftRight]
      have ihk := ih (k+1)
      change Nat.repeat ExtendedMantissa.shiftRightOne n { mantissa := m * 2^(n+(k+1)), roundBit := false, stickyBit := false } = _ at ihk
      change Nat.repeat ExtendedMantissa.shiftRightOne (n+1) { mantissa := m * 2^(n.succ+k), roundBit := false, stickyBit := false } = _
      have hexp : n.succ + k = n + (k + 1) := by omega
      rw [hexp]
      simp only [Nat.repeat]
      rw [ihk]
      simp only [ExtendedMantissa.shiftRightOne]
      rw [Nat.pow_succ]
      rw [← Nat.mul_assoc]
      rw [Nat.mul_comm (m * 2^k) 2]
      rw [Nat.mul_div_right (m * 2^k) (by decide : 0 < 2)]
      rw [Nat.mul_mod_right]
      rfl

private theorem mul_two_right (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (hnorm : 2^52 ≤ m) (hlt : m < 2^53) (hege : (-1074 : Int) ≤ e) :
    UnpackedFloat.mul Format.binary64 (.finite s m e hm)
      (.finite .positive 4503599627370496 (-51) (by decide)) =
      .finite s m (e+1) hm := by
  have hne : m ≠ 0 := by omega
  have hlog : m.log2 = 52 := (Nat.log2_eq_iff hne).mpr ⟨hnorm, hlt⟩
  have hpow : (2:Nat)^52 = 4503599627370496 := by decide
  have hmul : 4503599627370496 = (2:Nat)^52 := by omega
  have hlogprod : (m * 4503599627370496).log2 = 104 := by
    rw [hmul, log2_mul_two_pow m 52 hne, hlog]
  have hte : Format.binary64.targetExponent (totalExponent (m * 4503599627370496) (e + -51)) = e+1 := by
    simp [Format.targetExponent, totalExponent, hlogprod, Format.minExponent, Format.mantissaBits,
      Format.exponentBits]
    omega
  have hshift : (e + 1 - (e + -51)).toNat = 52 := by omega
  have hmax : max (e + 1 - (e + -51)) 0 = (52 : Int) := by omega
  have hte2 : Format.binary64.targetExponent (totalExponent m (e + -51 + 52)) = e+1 := by
    have hlogm : m.log2 = 52 := hlog
    simp [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits,
      Format.exponentBits, hlogm]
    omega
  have hshift2 : (Format.binary64.targetExponent (totalExponent m (e + -51 + 52)) - (e + -51 + 52)).toNat = 0 := by
    rw [hte2]
    omega
  have hshift3 : (e + 1 - (e + -51 + 52)).toNat = 0 := by omega
  have hmax2 : max (e + 1 - (e + -51 + 52)) 0 = (0 : Int) := by omega
  have hee : e + -51 + 52 = e + 1 := by omega
  have hsgn : s * Sign.positive = s := by cases s <;> rfl
  have hte3 : Format.binary64.targetExponent (totalExponent m (e + 1)) = e + 1 := by
    simpa [hee] using hte2
  simp [UnpackedFloat.mul, roundWithAccuracy, shiftToTargetExponent, shiftToExponent,
    ExtendedMantissa.ofMantissaAndAccuracy, ExtendedMantissa.shiftRightOne,
    hte, hlogprod, hpow, hshift, hmax, hte2, hshift2, hshift3, hmax2, hee, hte3, hne, em_shift_zero, hsgn,
    ExtendedMantissa.roundedMantissa, ExtendedMantissa.accuracy, Accuracy.roundToNearestEven,
    show m * 4503599627370496 = m * 2^52 by omega,
    em_shift_pow_gen m 52 0]

private theorem mul_two_left (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (hnorm : 2^52 ≤ m) (hlt : m < 2^53) (hege : (-1074 : Int) ≤ e) :
    UnpackedFloat.mul Format.binary64
      (.finite .positive 4503599627370496 (-51) (by decide)) (.finite s m e hm) =
      .finite s m (e+1) hm := by
  have h := mul_two_right s m e hm hnorm hlt hege
  have hsgn : Sign.positive * s = s * Sign.positive := by cases s <;> rfl
  simpa [UnpackedFloat.mul, hsgn, Nat.mul_comm, Int.add_comm] using h


private theorem round_succ (s : Sign) (m : Nat) (e : Int) (he : (-52 : Int) ≤ e) :
    round Format.binary64 s m (e+1) = scaleUF (round Format.binary64 s m e) := by
  let te := Format.binary64.targetExponent (totalExponent m e)
  have hteU : Format.binary64.targetExponent (totalExponent m e) = te := rfl
  have hteScaled : Format.binary64.targetExponent (totalExponent m (e+1)) = te + 1 := by
    dsimp [te]
    exact target_succ_ge m e (by omega : (-104 : Int) ≤ e)
  let sh := (e - te).toNat
  let mm := m <<< sh
  let ee := e - sh
  have hteGe : (-104 : Int) ≤ te := by
    dsimp [te]
    have hlog : 0 ≤ (m.log2 : Int) := by omega
    simp [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits,
      Format.exponentBits]
    omega
  have hee : (-104 : Int) ≤ ee := by
    dsimp [ee, sh]
    by_cases hle : e ≤ te
    · have hcast : (e - te).toNat = 0 := Int.toNat_of_nonpos (by omega)
      simp [hcast]
      omega
    · have hcast : ((e - te).toNat : Int) = e - te := Int.toNat_of_nonneg (by omega)
      omega
  have hInt : e + 1 - (te + 1) = e - te := by omega
  have hdecS : decreaseExponent m (e+1) (te+1) = (mm, ee+1) := by
    dsimp [decreaseExponent, mm, ee, sh]
    rw [hInt]
    apply Prod.ext
    · rfl
    · omega
  have hdecU : decreaseExponent m e te = (mm, ee) := by
    rfl
  simp [round, hteU, hteScaled, hdecS, hdecU]
  exact rwa_succ s mm ee hee

private theorem normalize_succ (z : Int) (e : Int) (he : (-52 : Int) ≤ e) (zsign : Sign) :
    normalize Format.binary64 z (e+1) zsign = scaleUF (normalize Format.binary64 z e zsign) := by
  cases hcmp : compare z 0 with
  | lt =>
      have hz : z < 0 := Int.compare_eq_lt.mp hcmp
      simp [normalize, hcmp]
      exact round_succ Sign.negative ((-z).toNat) e he
  | eq =>
      have hz : z = 0 := Int.compare_eq_eq.mp hcmp
      subst z
      simp [normalize, scaleUF]
  | gt =>
      have hz : 0 < z := Int.compare_eq_gt.mp hcmp
      simp [normalize, hcmp]
      exact round_succ Sign.positive z.toNat e he

private theorem add_succ (s1 s2 : Sign) (m1 m2 : Nat) (e1 e2 : Int)
    (hm1 : 0 < m1) (hm2 : 0 < m2) (he1 : (-52 : Int) ≤ e1) (he2 : (-52 : Int) ≤ e2) :
    UnpackedFloat.add Format.binary64 (.finite s1 m1 (e1+1) hm1) (.finite s2 m2 (e2+1) hm2) =
      scaleUF (UnpackedFloat.add Format.binary64 (.finite s1 m1 e1 hm1) (.finite s2 m2 e2 hm2)) := by
  let E := min e1 e2
  have hE : (-52 : Int) ≤ E := by
    dsimp [E]
    omega
  have hminS : min (e1+1) (e2+1) = E + 1 := by
    dsimp [E]
    by_cases h : e1 ≤ e2
    · rw [Int.min_eq_left h, Int.min_eq_left (by omega : e1 + 1 ≤ e2 + 1)]
    · have h' : e2 ≤ e1 := by omega
      rw [Int.min_eq_right h', Int.min_eq_right (by omega : e2 + 1 ≤ e1 + 1)]
  let d1 := (e1 - E).toNat
  let d2 := (e2 - E).toNat
  let a1 := m1 <<< d1
  let a2 := m2 <<< d2
  have hE1 : E ≤ e1 := by
    dsimp [E]
    exact Int.min_le_left e1 e2
  have hE2 : E ≤ e2 := by
    dsimp [E]
    exact Int.min_le_right e1 e2
  have hcast1 : ((e1 - E).toNat : Int) = e1 - E := Int.toNat_of_nonneg (by omega)
  have hcast2 : ((e2 - E).toNat : Int) = e2 - E := Int.toNat_of_nonneg (by omega)
  have hdec1U : decreaseExponent m1 e1 E = (a1, E) := by
    dsimp [decreaseExponent, a1, d1]
    apply Prod.ext
    · rfl
    · omega
  have hdec2U : decreaseExponent m2 e2 E = (a2, E) := by
    dsimp [decreaseExponent, a2, d2]
    apply Prod.ext
    · rfl
    · omega
  have hInt1 : e1 + 1 - (E + 1) = e1 - E := by omega
  have hInt2 : e2 + 1 - (E + 1) = e2 - E := by omega
  have hdec1S : decreaseExponent m1 (e1+1) (E+1) = (a1, E+1) := by
    dsimp [decreaseExponent, a1, d1]
    rw [hInt1]
    apply Prod.ext
    · rfl
    · omega
  have hdec2S : decreaseExponent m2 (e2+1) (E+1) = (a2, E+1) := by
    dsimp [decreaseExponent, a2, d2]
    rw [hInt2]
    apply Prod.ext
    · rfl
    · omega
  let M := s1.apply a1 + s2.apply a2
  simp [UnpackedFloat.add, hminS, hdec1S, hdec2S, hdec1U, hdec2U, M]
  exact normalize_succ M E hE .positive


private theorem abs_unpack (a : Float) {s : Sign} {m : Nat} {e : Int} {hm : 0 < m}
    (hup : (Float.toModel a).unpack = .finite s m e hm) :
    (Float.toModel (Float.abs a)).unpack = .finite .positive m e hm := by
  have hb := unpack_finite_bounds hup
  have hcanon : 2^52 ≤ m ∨ e = -1074 := by
    by_cases hlt : (-1074 : Int) < e
    · exact Or.inl (hb.2.2.2 hlt)
    · exact Or.inr (by omega)
  have hpack := unpack_pack_finite_canonical .positive m e hm hb.2.2.1 hb.1 hb.2.1 hcanon
  simp [Float.abs, Float.Model.abs, UnpackedFloat.abs, hup, hpack]

private theorem finite_ge_one_unpack (a : Float)
    (hfin : TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.finite a)
    (habs : (1 : Float) ≤ Float.abs a) :
    ∃ s m e hm, (Float.toModel a).unpack = .finite s m e hm ∧ (-52 : Int) ≤ e := by
  let up := (Float.toModel a).unpack
  have hfin' : up.isFinite = true := by
    simpa [TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.finite, Float.isFinite,
      Float.Model.isFinite, up] using hfin
  cases hu : up with
  | notANumber => simp [UnpackedFloat.isFinite, hu] at hfin'
  | infinity s => simp [UnpackedFloat.isFinite, hu] at hfin'
  | zero s =>
      have habsu : (Float.toModel (Float.abs a)).unpack = .zero .positive := by
        simpa [Float.abs, Float.Model.abs, UnpackedFloat.abs, up, hu] using unpack_pack_zero .positive
      have hone : (Float.toModel (1 : Float)).unpack = .finite .positive 4503599627370496 (-52) (by decide) := by rfl
      change (Float.toModel (1 : Float)) ≤ (Float.toModel (Float.abs a)) at habs
      simp only [LE.le, Float.Model.le, UnpackedFloat.le, hone, habsu,
        UnpackedFloat.compare, Option.any_eq_true, Ordering.isLE] at habs
      simp at habs
  | finite s m e hm =>
      have hge : (-52 : Int) ≤ e := by
        have habsu := abs_unpack (a := a) (s := s) (m := m) (e := e) (hm := hm) (by simpa [up] using hu)
        have hone : (Float.toModel (1 : Float)).unpack = .finite .positive 4503599627370496 (-52) (by decide) := by rfl
        change (Float.toModel (1 : Float)) ≤ (Float.toModel (Float.abs a)) at habs
        simp only [LE.le, Float.Model.le, UnpackedFloat.le, hone, habsu,
          UnpackedFloat.compare, Option.any_eq_true, Ordering.isLE] at habs
        by_cases hle : (-52 : Int) ≤ e
        · exact hle
        · have hlt : e < (-52 : Int) := by omega
          have hcmp : compare (-52 : Int) e = Ordering.gt := Int.compare_eq_gt.mpr hlt
          simp [hcmp] at habs
      exact ⟨s, m, e, hm, by simpa [up] using hu, hge⟩


private theorem finite_ge_one_unpack_bounded (a : Float)
    (hfin : TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.finite a)
    (habs : (1 : Float) ≤ Float.abs a)
    (hfin2 : TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.finite (2 * a)) :
    ∃ s m e hm, (Float.toModel a).unpack = .finite s m e hm ∧
      (-52 : Int) ≤ e ∧ e ≤ 970 ∧ 2^52 ≤ m ∧ m < 2^53 := by
  rcases finite_ge_one_unpack a hfin habs with ⟨s, m, e, hm, hup, hege⟩
  have hb := unpack_finite_bounds hup
  have hnorm : 2^52 ≤ m := hb.2.2.2 (by omega : (-1074 : Int) < e)
  have hmul := mul_two_left s m e hm hnorm hb.2.2.1 (by omega : (-1074 : Int) ≤ e)
  have htwo : (Float.toModel (2 : Float)).unpack =
      .finite .positive 4503599627370496 (-51) (by decide) := by rfl
  have h2model : Float.toModel (2 * a) = Float.Model.pack (.finite s m (e+1) hm) := by
    change Float.Model.pack (UnpackedFloat.mul Format.binary64 (Float.toModel (2 : Float)).unpack
      (Float.toModel a).unpack) = Float.Model.pack (.finite s m (e+1) hm)
    rw [htwo, hup, hmul]
  have hle : e ≤ 970 := by
    by_cases hle' : e ≤ 970
    · exact hle'
    · have heq : e = 971 := by omega
      have hov := pack_overflow_exp (s := s) (m := m) (e := e+1) (hm := hm) (by omega : (972 : Int) ≤ e+1)
      have h2fin : ((Float.toModel (2 * a)).unpack).isFinite = true := by
        simpa [TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.finite, Float.isFinite,
          Float.Model.isFinite] using hfin2
      have h2inf : (Float.toModel (2 * a)).unpack = .infinity s := by
        rw [h2model]
        exact hov
      simp [h2inf, UnpackedFloat.isFinite] at h2fin
  exact ⟨s, m, e, hm, hup, hege, hle, hnorm, hb.2.2.1⟩


private theorem shift_bounds (n k p : Nat) (hn : 0 < n) (hlog : n.log2 = p + k) :
    2^p ≤ n >>> k ∧ n >>> k < 2^(p+1) := by
  have hnne : n ≠ 0 := by omega
  have hlow : 2 ^ n.log2 ≤ n := Nat.log2_self_le hnne
  have hhigh : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hnne).mp (by omega : n.log2 < n.log2 + 1)
  rw [Nat.shiftRight_eq_div_pow]
  constructor
  · rw [Nat.le_div_iff_mul_le (Nat.two_pow_pos k)]
    rw [← Nat.pow_add]
    rw [← hlog]
    exact hlow
  · rw [Nat.div_lt_iff_lt_mul (Nat.two_pow_pos k)]
    rw [← Nat.pow_add]
    have hp : p + 1 + k = p + k + 1 := by omega
    rw [hp, ← hlog]
    exact hhigh

private theorem em_shift_mantissa (m n : Nat) :
    (({ mantissa := m, roundBit := false, stickyBit := false } : ExtendedMantissa) >>> n).mantissa = m >>> n := by
  induction n with
  | zero => rfl
  | succ n ih =>
      change (Nat.repeat ExtendedMantissa.shiftRightOne n
        ({ mantissa := m, roundBit := false, stickyBit := false } : ExtendedMantissa)).mantissa = m >>> n at ih
      simp only [HShiftRight.hShiftRight, Nat.repeat, ExtendedMantissa.shiftRightOne]
      rw [ih]
      rfl

private theorem rwa_finite_canonical {s : Sign} {m : Nat} {e : Int}
    {s2 : Sign} {m2 : Nat} {e2 : Int}
    (hm : 0 < m) (hmnorm : 2^52 ≤ m) (he : (-104 : Int) ≤ e) (hm2 : 0 < m2)
    (h : roundWithAccuracy Format.binary64 s m e Accuracy.exact = .finite s2 m2 e2 hm2) :
    s2 = s ∧ 2^52 ≤ m2 ∧ m2 < 2^53 ∧ (-1074 : Int) ≤ e2 := by
  let te := Format.binary64.targetExponent (totalExponent m e)
  have hloglow : 52 ≤ m.log2 := (Nat.le_log2 (by omega : m ≠ 0)).mpr hmnorm
  have hte : te = (m.log2 : Int) + 1 + e - 53 := by
    dsimp [te]
    simp [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits,
      Format.exponentBits]
    rw [Int.max_eq_left (by omega : (-1074 : Int) ≤ (m.log2 : Int) + 1 + e - 53)]
  let sh := (te - e).toNat
  have hsh : (sh : Int) = (m.log2 : Int) - 52 := by
    dsimp [sh]
    rw [hte]
    omega
  have hlogm : m.log2 = 52 + sh := by omega
  let em1 := ExtendedMantissa.ofMantissaAndAccuracy m Accuracy.exact >>> sh
  let e1 := e + sh
  have he1 : (-104 : Int) ≤ e1 := by dsimp [e1]; omega
  have hm1 : em1.mantissa = m >>> sh := by
    dsimp [em1]
    simpa [ExtendedMantissa.ofMantissaAndAccuracy] using em_shift_mantissa m sh
  have hm1b := shift_bounds m sh 52 hm hlogm
  have hm1b' : 2^52 ≤ em1.mantissa ∧ em1.mantissa < 2^53 := by simpa [hm1] using hm1b
  let r := em1.roundedMantissa
  have hmodlt : em1.mantissa % 2 < 2 := Nat.mod_lt _ (by decide : 0 < 2)
  have hmod : em1.mantissa % 2 ≤ 1 := by omega
  have hrb : 2^52 ≤ r ∧ r ≤ 2^53 := by
    dsimp [r]
    cases hem : em1 with
    | mk mant rb sb =>
      have hmant_low : 2^52 ≤ mant := by simpa [hem] using hm1b'.1
      have hmant_high : mant < 2^53 := by simpa [hem] using hm1b'.2
      have hmod' : mant % 2 ≤ 1 := by simpa [hem] using hmod
      cases rb <;> cases sb <;>
        simp [ExtendedMantissa.roundedMantissa, ExtendedMantissa.accuracy,
          Accuracy.roundToNearestEven, hem] at hmant_low hmant_high hmod' ⊢ <;> omega
  have hrpos : 0 < r := by omega
  have hlogrlow : 52 ≤ r.log2 := (Nat.le_log2 (by omega : r ≠ 0)).mpr hrb.1
  have hrlt54 : r < 2^54 := by
    rw [show (2:Nat)^54 = 2 * 2^53 by decide]
    omega
  have hlogrhigh : r.log2 ≤ 53 := by
    have hlt : r.log2 < 54 := (Nat.log2_lt (by omega : r ≠ 0)).mpr hrlt54
    omega
  let te2 := Format.binary64.targetExponent (totalExponent r e1)
  have hte2 : te2 = (r.log2 : Int) + 1 + e1 - 53 := by
    dsimp [te2]
    simp [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits,
      Format.exponentBits]
    rw [Int.max_eq_left (by omega : (-1074 : Int) ≤ (r.log2 : Int) + 1 + e1 - 53)]
  let sh2 := (te2 - e1).toNat
  have hsh2 : (sh2 : Int) = (r.log2 : Int) - 52 := by
    dsimp [sh2]
    rw [hte2]
    omega
  have hlogr : r.log2 = 52 + sh2 := by omega
  let em2 := ExtendedMantissa.ofMantissaAndAccuracy r Accuracy.exact >>> sh2
  let ef := e1 + sh2
  have hem2 : em2.mantissa = r >>> sh2 := by
    dsimp [em2]
    simpa [ExtendedMantissa.ofMantissaAndAccuracy] using em_shift_mantissa r sh2
  have hm2b := shift_bounds r sh2 52 hrpos hlogr
  have hshift1 : shiftToExponent m e Accuracy.exact te = (em1, e1) := by rfl
  have hshift2 : shiftToExponent r e1 Accuracy.exact te2 = (em2, ef) := by rfl
  have hcalc : roundWithAccuracy Format.binary64 s m e Accuracy.exact =
      if hz : em2.mantissa = 0 then .zero s else .finite s em2.mantissa ef (Nat.pos_of_ne_zero hz) := by
    simp [roundWithAccuracy, shiftToTargetExponent,
      show Format.binary64.targetExponent (totalExponent m e) = te by rfl,
      show Format.binary64.targetExponent (totalExponent r e1) = te2 by rfl,
      hshift1, hshift2, show em1.roundedMantissa = r by rfl]
  rw [hcalc] at h
  by_cases hz : em2.mantissa = 0
  · simp [hz] at h
  · simp [hz] at h
    rcases h with ⟨hs, hmEq, heEq⟩
    subst s2
    subst m2
    subst e2
    constructor
    · rfl
    constructor
    · simpa [hem2] using hm2b.1
    constructor
    · simpa [hem2] using hm2b.2
    · omega

private theorem round_finite_canonical (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (he : (-52 : Int) ≤ e) {s2 : Sign} {m2 : Nat} {e2 : Int} {hm2 : 0 < m2}
    (h : round Format.binary64 s m e = .finite s2 m2 e2 hm2) :
    2^52 ≤ m2 ∧ m2 < 2^53 ∧ (-1074 : Int) ≤ e2 := by
  let te := Format.binary64.targetExponent (totalExponent m e)
  have hteForm : te = (m.log2 : Int) + 1 + e - 53 := by
    dsimp [te]
    simp [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits,
      Format.exponentBits]
    rw [Int.max_eq_left (by omega : (-1074 : Int) ≤ (m.log2 : Int) + 1 + e - 53)]
  let sh1 := (e - te).toNat
  let mm := m <<< sh1
  let ee := e - sh1
  have hdec : decreaseExponent m e te = (mm, ee) := by rfl
  have hteGe : (-104 : Int) ≤ te := by omega
  have hmmpos : 0 < mm := by
    dsimp [mm]
    rw [Nat.shiftLeft_eq]
    exact Nat.mul_pos hm (Nat.two_pow_pos _)
  have hmmnorm : 2^52 ≤ mm := by
    dsimp [mm]
    by_cases hle : e ≤ te
    · have hcast : (e - te).toNat = 0 := Int.toNat_of_nonpos (by omega)
      simp [sh1, hcast]
      have hlog : 52 ≤ m.log2 := by omega
      exact (Nat.le_log2 (by omega : m ≠ 0)).mp hlog
    · have hcast : ((e - te).toNat : Int) = e - te := Int.toNat_of_nonneg (by omega)
      have hlog : m.log2 + (e - te) = 52 := by omega
      have hlogNat : m.log2 + (e - te).toNat = 52 := by omega
      have hres : (m <<< (e - te).toNat).log2 = m.log2 + (e - te).toNat := by
        simpa [Nat.shiftLeft_eq] using log2_mul_two_pow m (e - te).toNat (by omega : m ≠ 0)
      have hlogeq : (mm).log2 = 52 := by
        change (m <<< (e - te).toNat).log2 = 52
        rw [hres]
        exact hlogNat
      change 2^52 ≤ m <<< (e - te).toNat
      have hne : m <<< (e - te).toNat ≠ 0 := by
        dsimp [mm] at hmmpos
        exact Nat.ne_of_gt hmmpos
      exact (Nat.le_log2 hne).mp (by rw [hlogeq]; omega)
  have hee : (-104 : Int) ≤ ee := by
    dsimp [ee, sh1]
    by_cases hle : e ≤ te
    · have hcast : (e - te).toNat = 0 := Int.toNat_of_nonpos (by omega)
      simp [hcast]
      omega
    · have hcast : ((e - te).toNat : Int) = e - te := Int.toNat_of_nonneg (by omega)
      omega
  have hroundU : round Format.binary64 s m e = roundWithAccuracy Format.binary64 s mm ee Accuracy.exact := by
    simp [round, show Format.binary64.targetExponent (totalExponent m e) = te by rfl, hdec]
  rw [hroundU] at h
  exact (rwa_finite_canonical hmmpos hmmnorm hee hm2 h).2


private theorem normalize_finite_canonical (z : Int) (e : Int) (he : (-52 : Int) ≤ e)
    {s : Sign} {m : Nat} {e2 : Int} {hm : 0 < m}
    (h : normalize Format.binary64 z e .positive = .finite s m e2 hm) :
    2^52 ≤ m ∧ m < 2^53 ∧ (-1074 : Int) ≤ e2 := by
  cases hz : compare z 0 with
  | lt =>
      have hneg : z < 0 := Int.compare_eq_lt.mp hz
      simp [normalize, hz] at h
      exact round_finite_canonical .negative ((-z).toNat) e (by omega) he h
  | eq =>
      simp [normalize, hz] at h
  | gt =>
      have hpos : 0 < z := Int.compare_eq_gt.mp hz
      simp [normalize, hz] at h
      exact round_finite_canonical .positive z.toNat e (by omega) he h

private theorem add_finite_canonical (s1 s2 : Sign) (m1 m2 : Nat) (e1 e2 : Int)
    (hn1 : 2^52 ≤ m1) (hn2 : 2^52 ≤ m2) (hl1 : m1 < 2^53) (hl2 : m2 < 2^53)
    (he1 : (-52 : Int) ≤ e1) (he2 : (-52 : Int) ≤ e2)
    {s : Sign} {m : Nat} {e : Int} {hm : 0 < m}
    (h : UnpackedFloat.add Format.binary64 (.finite s1 m1 e1 (by omega : 0 < m1))
      (.finite s2 m2 e2 (by omega : 0 < m2)) = .finite s m e hm) :
    2^52 ≤ m ∧ m < 2^53 ∧ (-1074 : Int) ≤ e := by
  let E := min e1 e2
  let M := s1.apply (decreaseExponent m1 e1 E).fst +
    s2.apply (decreaseExponent m2 e2 E).fst
  have hE : (-52 : Int) ≤ E := by dsimp [E]; omega
  have hadd : UnpackedFloat.add Format.binary64 (.finite s1 m1 e1 (by omega : 0 < m1))
      (.finite s2 m2 e2 (by omega : 0 < m2)) = normalize Format.binary64 M E .positive := by rfl
  rw [hadd] at h
  exact normalize_finite_canonical M E hE h

private theorem mul_two_scale (u : UnpackedFloat)
    (hcanon : match u with
      | .finite _ m e _ => 2^52 ≤ m ∧ m < 2^53 ∧ (-1074 : Int) ≤ e
      | _ => True) :
    UnpackedFloat.mul Format.binary64
      (.finite .positive 4503599627370496 (-51) (by decide)) u = scaleUF u := by
  cases u with
  | notANumber => simp [UnpackedFloat.mul, scaleUF]
  | infinity s =>
      have hsgn : Sign.positive * s = s := by cases s <;> rfl
      simp [UnpackedFloat.mul, scaleUF, hsgn]
  | zero s =>
      have hsgn : Sign.positive * s = s := by cases s <;> rfl
      simp [UnpackedFloat.mul, scaleUF, hsgn]
  | finite s m e hm =>
      exact mul_two_left s m e hm hcanon.1 hcanon.2.1 hcanon.2.2

private theorem pack_finite_overflow_eq_inf {s : Sign} {m : Nat} {e : Int} {hm : 0 < m}
    (he : (972 : Int) ≤ e) :
    Float.Model.pack (.finite s m e hm) = Float.Model.pack (.infinity s) := by
  have hbias : Format.binary64.exponentBias = 1023 := by decide
  have hmb : Format.binary64.mantissaBitsWithoutImplicit = 52 := by decide
  have heb : Format.binary64.exponentBits = 11 := by decide
  have hnonneg : (0 : Int) ≤ e + 1023 + 52 := by omega
  have hcond : 2047 ≤ (e + 1023 + 52).toNat := by omega
  rw [Float.Model.mk.injEq]
  simp [Float.Model.pack, UnpackedFloat.pack, hbias, hmb, heb, hcond, packedInfinity]

private theorem pack_mul_two_add (s1 s2 : Sign) (m1 m2 : Nat) (e1 e2 : Int)
    (hn1 : 2^52 ≤ m1) (hn2 : 2^52 ≤ m2) (hl1 : m1 < 2^53) (hl2 : m2 < 2^53)
    (he1 : (-52 : Int) ≤ e1) (he2 : (-52 : Int) ≤ e2) :
    Float.Model.pack (UnpackedFloat.mul Format.binary64
      (.finite .positive 4503599627370496 (-51) (by decide))
      (Float.Model.pack (UnpackedFloat.add Format.binary64
        (.finite s1 m1 e1 (by omega : 0 < m1)) (.finite s2 m2 e2 (by omega : 0 < m2)))).unpack) =
    Float.Model.pack (scaleUF (UnpackedFloat.add Format.binary64
      (.finite s1 m1 e1 (by omega : 0 < m1)) (.finite s2 m2 e2 (by omega : 0 < m2)))) := by
  generalize huDef : UnpackedFloat.add Format.binary64
      (.finite s1 m1 e1 (by omega : 0 < m1)) (.finite s2 m2 e2 (by omega : 0 < m2)) = u
  cases hu : u with
  | notANumber =>
      have hn : u.isNaN = false := by
        rw [← huDef]
        change (normalize Format.binary64
          (s1.apply (decreaseExponent m1 e1 (min e1 e2)).fst +
            s2.apply (decreaseExponent m2 e2 (min e1 e2)).fst)
          (min e1 e2) .positive).isNaN = false
        exact normalize_isNaN_false _ _ _
      rw [hu] at hn
      simp [UnpackedFloat.isNaN] at hn
  | infinity s =>
      rw [unpack_pack_inf s]
      cases s <;> rfl
  | zero s =>
      rw [unpack_pack_zero s]
      cases s <;> rfl
  | finite s m e hm =>
      have hc := add_finite_canonical s1 s2 m1 m2 e1 e2 hn1 hn2 hl1 hl2 he1 he2 (by simpa [hu] using huDef.trans hu)
      by_cases h971 : e ≤ 971
      · have hup := unpack_pack_finite_canonical s m e hm hc.2.1 hc.2.2 h971 (Or.inl hc.1)
        rw [hup]
        rw [mul_two_left s m e hm hc.1 hc.2.1 hc.2.2]
        rfl
      · have hov972 : (972 : Int) ≤ e := by omega
        have hup := pack_overflow_exp (s := s) (m := m) (e := e) (hm := hm) hov972
        have hoveq2 := pack_finite_overflow_eq_inf (s := s) (m := m) (e := e+1) (hm := hm) (by omega : (972 : Int) ≤ e+1)
        have hsgn : Sign.positive * s = s := by cases s <;> rfl
        rw [hup]
        simp [scaleUF, UnpackedFloat.mul, hoveq2, hsgn]


theorem sound : ExactBinaryPowerScale.Goal := by
  intro a b hfa hfb hf2a hf2b ha1 hb1
  rcases finite_ge_one_unpack_bounded a hfa ha1 hf2a with
    ⟨sa, ma, ea, hma, hua, hea,heaLe, hnma, hlma⟩
  rcases finite_ge_one_unpack_bounded b hfb hb1 hf2b with
    ⟨sb, mb, eb, hmb, hub, heb,hebLe, hnmb, hlmb⟩
  let ua : UnpackedFloat := .finite sa ma ea hma
  let ub : UnpackedFloat := .finite sb mb eb hmb
  have htwo : (Float.toModel (2 : Float)).unpack =
      .finite .positive 4503599627370496 (-51) (by decide) := by rfl
  have hsum : Float.toModel (a + b) = Float.Model.pack (UnpackedFloat.add Format.binary64 ua ub) := by
    change Float.Model.pack (UnpackedFloat.add Format.binary64 (Float.toModel a).unpack
      (Float.toModel b).unpack) = Float.Model.pack (UnpackedFloat.add Format.binary64 ua ub)
    rw [hua, hub]
  have hleft : Float.toModel (2 * (a + b)) =
      Float.Model.pack (scaleUF (UnpackedFloat.add Format.binary64 ua ub)) := by
    change Float.Model.pack (UnpackedFloat.mul Format.binary64 (Float.toModel (2 : Float)).unpack
      (Float.toModel (a + b)).unpack) =
      Float.Model.pack (scaleUF (UnpackedFloat.add Format.binary64 ua ub))
    rw [htwo, hsum]
    exact pack_mul_two_add sa sb ma mb ea eb hnma hnmb hlma hlmb hea heb
  have h2a : Float.toModel (2 * a) = Float.Model.pack (scaleUF ua) := by
    change Float.Model.pack (UnpackedFloat.mul Format.binary64 (Float.toModel (2 : Float)).unpack
      (Float.toModel a).unpack) = Float.Model.pack (scaleUF ua)
    rw [htwo, hua]
    rw [mul_two_left sa ma ea hma hnma hlma (by omega : (-1074 : Int) ≤ ea)]
    rfl
  have h2b : Float.toModel (2 * b) = Float.Model.pack (scaleUF ub) := by
    change Float.Model.pack (UnpackedFloat.mul Format.binary64 (Float.toModel (2 : Float)).unpack
      (Float.toModel b).unpack) = Float.Model.pack (scaleUF ub)
    rw [htwo, hub]
    rw [mul_two_left sb mb eb hmb hnmb hlmb (by omega : (-1074 : Int) ≤ eb)]
    rfl
  have huaUp : (Float.Model.pack (scaleUF ua)).unpack = scaleUF ua := by
    dsimp [ua, scaleUF]
    exact unpack_pack_finite_canonical sa ma (ea+1) hma hlma (by omega) (by omega) (Or.inl hnma)
  have hubUp : (Float.Model.pack (scaleUF ub)).unpack = scaleUF ub := by
    dsimp [ub, scaleUF]
    exact unpack_pack_finite_canonical sb mb (eb+1) hmb hlmb (by omega) (by omega) (Or.inl hnmb)
  have hright : Float.toModel (2 * a + 2 * b) =
      Float.Model.pack (scaleUF (UnpackedFloat.add Format.binary64 ua ub)) := by
    change Float.Model.pack (UnpackedFloat.add Format.binary64
      (Float.toModel (2 * a)).unpack (Float.toModel (2 * b)).unpack) =
      Float.Model.pack (scaleUF (UnpackedFloat.add Format.binary64 ua ub))
    rw [h2a, h2b, huaUp, hubUp]
    change Float.Model.pack (UnpackedFloat.add Format.binary64
      (.finite sa ma (ea+1) hma) (.finite sb mb (eb+1) hmb)) =
      Float.Model.pack (scaleUF (UnpackedFloat.add Format.binary64
        (.finite sa ma ea hma) (.finite sb mb eb hmb)))
    rw [add_succ sa sb ma mb ea eb hma hmb hea heb]
  unfold same bits
  change (Float.toModel (2 * (a + b))).toBits = (Float.toModel (2 * a + 2 * b)).toBits
  rw [hleft, hright]

end Nov26.Proof_FloatingReduction_ExactBinaryPowerScale
