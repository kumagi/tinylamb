import Std
import TinylambOpt.ResearchProofs.JitKernels.Common_FloatingReduction_Bits

/-! Scaling facts for the IEEE-754 `Float.Model` rounding pipeline.

Doubling a canonical binary64 finite value increases its unpacked exponent by
one and leaves the mantissa unchanged. Rounding is equivariant under the same
exponent shift while the exponent stays above the subnormal cap. These facts
let a JIT kernel distribute a factor of two over a finite reduction step
without changing the observable result bits.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReductionScale

open Float.Model
open Float.Model.UnpackedFloat
open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionBits
open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionCommon

/- Natural-number logarithm facts used by the packing pipeline. -/

theorem log2_mul_pow2 (m n : Nat) (hm : 0 < m) : (m * 2 ^ n).log2 = m.log2 + n := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [Nat.pow_succ]
      rw [show m * (2 ^ n * 2) = (m * 2 ^ n) * 2 by ac_rfl]
      rw [Nat.log2_def]
      have hm1 : 1 ≤ m := by omega
      have hp : 1 ≤ 2 ^ n := Nat.one_le_pow n 2 (by decide)
      have hprod : 1 ≤ m * 2 ^ n := Nat.mul_le_mul hm1 hp
      have hge : 2 ≤ (m * 2 ^ n) * 2 := by omega
      rw [ite_eq_left hge]
      rw [show ((m * 2 ^ n) * 2) / 2 = m * 2 ^ n by omega]
      rw [ih]
      omega

theorem log2_eq_of_bounds (m k : Nat) (hlo : 2 ^ k ≤ m) (hhi : m < 2 ^ (k + 1)) : m.log2 = k := by
  induction k generalizing m with
  | zero =>
      have hm : m = 1 := by omega
      subst m
      simp [Nat.log2_def]
  | succ k ih =>
      rw [Nat.log2_def]
      have hp : 2 ^ 1 ≤ 2 ^ (k + 1) := Nat.pow_le_pow_right (by omega : 0 < 2) (by omega)
      have hge : 2 ≤ m := by omega
      rw [ite_eq_left hge]
      have hlo' : 2 ^ k ≤ m / 2 := by omega
      have hhi' : m / 2 < 2 ^ (k + 1) := by omega
      rw [ih (m / 2) hlo' hhi']

/- Repeated right shifts of an exactly-marked mantissa that is a multiple of a
   power of two discard exactly that many low zero bits. -/

theorem em_shift_mul_pow2 (m n : Nat) :
    (ExtendedMantissa.ofMantissaAndAccuracy (m * 2 ^ n) Accuracy.exact >>> n) =
      ⟨m, false, false⟩ := by
  change Nat.repeat ExtendedMantissa.shiftRightOne n
      (ExtendedMantissa.ofMantissaAndAccuracy (m * 2 ^ n) Accuracy.exact) = ⟨m, false, false⟩
  induction n generalizing m with
  | zero => simp [Nat.repeat, ExtendedMantissa.ofMantissaAndAccuracy]
  | succ n ih =>
      rw [Nat.pow_succ]
      simp only [Nat.repeat]
      rw [show m * (2 ^ n * 2) = (m * 2) * 2 ^ n by ac_rfl]
      rw [ih]
      simp [ExtendedMantissa.shiftRightOne]

/- When the bias computation is not capped by the minimum exponent, increasing
   the exponent by one increases the computed target exponent by one while the
   required right shift is unchanged. -/

theorem shiftToTarget_succ (m : Nat) (e : Int) (acc : Accuracy)
    (h : -1021 ≤ m.log2 + 1 + e) :
    shiftToTargetExponent Format.binary64 m (e + 1) acc =
      (let p := shiftToTargetExponent Format.binary64 m e acc; (p.1, p.2 + 1)) := by
  unfold shiftToTargetExponent shiftToExponent
  simp only [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits, Nat.reduceAdd]
  have hpow : (2 ^ (11 - 1) : Nat) = 1024 := by decide
  have hcap : 3 - 2 ^ (11 - 1) - (↑(53 : Nat) : Int) ≤ ↑m.log2 + 1 + e - (↑(53 : Nat) : Int) := by omega
  have hcap' : 3 - 2 ^ (11 - 1) - (↑(53 : Nat) : Int) ≤ ↑m.log2 + 1 + (e + 1) - (↑(53 : Nat) : Int) := by omega
  rw [Int.max_eq_left hcap']
  rw [Int.max_eq_left hcap]
  congr 1
  · congr 1
    omega
  · omega

theorem roundWA_succ (s : Sign) (m : Nat) (e : Int) (acc : Accuracy)
    (h1 : -1021 ≤ m.log2 + 1 + e) (hlow : -1022 ≤ e) :
    roundWithAccuracy Format.binary64 s m (e + 1) acc =
      (match roundWithAccuracy Format.binary64 s m e acc with
       | .finite s' m' e' hm => .finite s' m' (e' + 1) hm
       | x => x) := by
  unfold roundWithAccuracy
  rw [shiftToTarget_succ m e acc h1]
  let p := shiftToTargetExponent Format.binary64 m e acc
  have h2 : -1021 ≤ p.1.roundedMantissa.log2 + 1 + p.2 := by
    have hp2 : p.2 ≥ e := by
      dsimp [p]
      unfold shiftToTargetExponent shiftToExponent
      simp only
      omega
    have hlog : 0 ≤ p.1.roundedMantissa.log2 := by omega
    omega
  have h2' := shiftToTarget_succ p.1.roundedMantissa p.2 Accuracy.exact h2
  let q := shiftToTargetExponent Format.binary64 p.1.roundedMantissa p.2 Accuracy.exact
  let q' := shiftToTargetExponent Format.binary64 p.1.roundedMantissa (p.2 + 1) Accuracy.exact
  have hq : q' = (q.1, q.2 + 1) := by simpa [q, q'] using h2'
  change (if h : q'.1.mantissa = 0 then zero s else finite s q'.1.mantissa q'.2 (Nat.pos_of_ne_zero h)) =
    (match (if h : q.1.mantissa = 0 then zero s else finite s q.1.mantissa q.2 (Nat.pos_of_ne_zero h)) with
     | .finite s' m' e' hm => .finite s' m' (e' + 1) hm
     | x => x)
  rw [hq]
  by_cases hzero : q.1.mantissa = 0 <;> simp [hzero]

/- A canonical 53-bit mantissa already sits at its target exponent. -/

theorem shift_to_target_canonical (m : Nat) (e : Int) (hlog : m.log2 = 52) (he : -51 ≤ e) :
    shiftToTargetExponent Format.binary64 m e Accuracy.exact = (⟨m, false, false⟩, e) := by
  unfold shiftToTargetExponent shiftToExponent
  simp only [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits, Nat.reduceAdd]
  rw [hlog]
  have hcap : 3 - 2 ^ (11 - 1) - (↑(53 : Nat) : Int) ≤ (↑(52 : Nat) : Int) + 1 + e - (↑(53 : Nat) : Int) := by omega
  rw [Int.max_eq_left hcap]
  have hshift : (↑(52 : Nat) : Int) + 1 + e - (↑(53 : Nat) : Int) - e = 0 := by omega
  rw [hshift]
  simp [HShiftRight.hShiftRight, Nat.repeat, ExtendedMantissa.ofMantissaAndAccuracy]

/- Multiplying a canonical finite unpacked value by two increments its
   exponent and keeps its mantissa. -/

theorem shift_to_target_mul_two (m : Nat) (e : Int) (hm : 0 < m) (hlog : m.log2 = 52) (he : -52 ≤ e) :
    shiftToTargetExponent Format.binary64 (m * 4503599627370496) (e + -51) Accuracy.exact =
      (⟨m, false, false⟩, e + 1) := by
  unfold shiftToTargetExponent shiftToExponent
  simp only [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits, Nat.reduceAdd]
  rw [log2_mul_pow2 m 52 hm, hlog]
  have hcap : 3 - 2 ^ (11 - 1) - (↑(53 : Nat) : Int) ≤ (↑(104 : Nat) : Int) + 1 + (e + -51) - (↑(53 : Nat) : Int) := by omega
  rw [Int.max_eq_left hcap]
  have hshift : (↑(104 : Nat) : Int) + 1 + (e + -51) - (↑(53 : Nat) : Int) - (e + -51) = 52 := by omega
  rw [hshift]
  rw [show (52 : Int).toNat = 52 by decide]
  rw [show 4503599627370496 = 2 ^ 52 by decide]
  rw [em_shift_mul_pow2]
  congr 1
  omega

theorem roundWA_mul_two (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (hlog : m.log2 = 52) (he : -52 ≤ e) :
    roundWithAccuracy Format.binary64 s (m * 4503599627370496) (e + -51) Accuracy.exact =
      .finite s m (e + 1) hm := by
  unfold roundWithAccuracy
  rw [shift_to_target_mul_two m e hm hlog he]
  simp only
  simp only [ExtendedMantissa.roundedMantissa, Accuracy.roundToNearestEven, ExtendedMantissa.accuracy]
  have hm0 : m ≠ 0 := by omega
  simp [shift_to_target_canonical m (e + 1) hlog (by omega), hm0]


/- Doubling a unpacked finite value. -/

def dbl : UnpackedFloat → UnpackedFloat
  | .finite s m e hm => .finite s m (e + 1) hm
  | x => x

theorem log2_shiftLeft (m n : Nat) (hm : 0 < m) : (m <<< n).log2 = m.log2 + n := by
  rw [Nat.shiftLeft_eq]
  exact log2_mul_pow2 m n hm

theorem pow_log2_le (n : Nat) (hn : 0 < n) : 2 ^ n.log2 ≤ n := by
  rw [Nat.log2_def]
  by_cases h : 2 ≤ n
  · rw [ite_eq_left h]
    rw [Nat.pow_succ]
    have hi := pow_log2_le (n / 2) (by omega)
    have h2 : 2 * 2 ^ (n / 2).log2 ≤ 2 * (n / 2) := Nat.mul_le_mul_left 2 hi
    have h3 : 2 * (n / 2) ≤ n := by omega
    exact Nat.le_trans (by simpa [Nat.mul_comm] using h2) h3
  · have hn1 : n = 1 := by omega
    subst n
    simp
termination_by n

theorem lt_pow_succ_log2 (n : Nat) (hn : 0 < n) : n < 2 ^ (n.log2 + 1) := by
  rw [Nat.log2_def]
  by_cases h : 2 ≤ n
  · rw [ite_eq_left h]
    rw [Nat.pow_succ]
    rw [Nat.pow_succ]
    have hi := lt_pow_succ_log2 (n / 2) (by omega)
    have hp : 2 ^ ((n / 2).log2 + 1) = 2 * 2 ^ (n / 2).log2 := by rw [Nat.pow_succ]; omega
    rw [hp] at hi
    omega
  · have hn1 : n = 1 := by omega
    subst n
    simp
termination_by n

/- Repacking a canonical finite unpacked value that did not overflow recovers
   exactly the original exponent and mantissa. -/

theorem append_one_ofNat52_toNat (m : Nat) (hlo : 4503599627370496 ≤ m)
    (hhi : m < 9007199254740992) :
    (1#1 ++ BitVec.ofNat 52 m : BitVec 53).toNat = m := by
  rw [BitVec.toNat_append]
  simp only [BitVec.toNat_ofNat]
  have hrlt : m - 4503599627370496 < 4503599627370496 := by omega
  rw [show m % 4503599627370496 = m - 4503599627370496 by omega]
  rw [show (1 : Nat) <<< 52 = 4503599627370496 by decide]
  rw [show 4503599627370496 ||| (m - 4503599627370496) = 4503599627370496 + (m - 4503599627370496) by
    exact (Nat.two_pow_add_eq_or_of_lt (i := 52) (by simpa using hrlt) 1).symm]
  omega

theorem ofNat11_toNat_sub_bias (e : Int) (hb_lt : (e + 1075).toNat < 2047) (hb_pos : 0 < (e + 1075).toNat) :
    (BitVec.ofNat 11 (e + 1075).toNat).toNat - 1075 = e := by
  simp only [BitVec.toNat_ofNat, Nat.mod_eq_of_lt (by omega : (e + 1075).toNat < 2 ^ 11)]
  omega

theorem unpack_pack_finite (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (hlog : m.log2 = 52) (he : -1074 ≤ e)
    (hfin : (Float.Model.pack (.finite s m e hm)).isFinite = true) :
    (Float.Model.pack (.finite s m e hm)).unpack = .finite s m e hm := by
  have hlo : 4503599627370496 ≤ m := by simpa [hlog] using pow_log2_le m hm
  have hhi : m < 9007199254740992 := by simpa [hlog] using lt_pow_succ_log2 m hm
  unfold Float.Model.pack Float.Model.unpack
  simp only [UnpackedFloat.pack]
  rw [hlog]
  simp only [Format.exponentBias, Format.mantissaBits]
  split
  · rename_i hov
    have hbad : (Float.Model.pack (.finite s m e hm)).isFinite = false := by
      unfold Float.Model.pack
      simp only [UnpackedFloat.pack, hlog]
      split
      · have hinf : UnpackedFloat.unpack Format.binary64 (packedInfinity Format.binary64 s) = .infinity s := by
          simpa [Float.Model.unpack, Float.Model.pack, UnpackedFloat.pack] using (FloatingReductionCommon.unpack_pack_inf s)
        simp [Float.Model.unpack, Float.Model.isFinite, hinf, UnpackedFloat.isFinite]
      · rename_i hn
        exfalso
        apply hn
        simpa [Format.exponentBias] using hov
    rw [hbad] at hfin
    contradiction
  · rename_i hnov
    have hb_lt : (e + 1075).toNat < 2047 := by omega
    have hb_pos : 0 < (e + 1075).toNat := by omega
    have hneall : BitVec.ofNat 11 (e + 1075).toNat ≠ 2047#11 := ofNat11_ne_2047 hb_lt
    have hne0 : BitVec.ofNat 11 (e + 1075).toNat ≠ 0#11 := by
      intro hz
      have ht := congrArg BitVec.toNat hz
      simp [BitVec.toNat_ofNat, Nat.mod_eq_of_lt (by omega : (e + 1075).toNat < 2 ^ 11)] at ht
      omega
    simp
    simp only [UnpackedFloat.unpack, unpackExponent_packComponents, unpackMantissa_packComponents,
      unpackSign_pc64, hneall, hne0, ↓reduceIte,
      show (e + (1023 : Int) + 52) = e + 1075 by omega,
      show max (e + 1075) 0 = e + 1075 by omega,
      show (e + 1075) % 2048 = e + 1075 by omega,
      show (↑Format.binary64.exponentBias + (52 : Nat) : Int) = 1075 by native_decide,
      show max (e + 1075) 0 % 2048 = e + 1075 by omega,
      sign_of_toBitVec,
      hneall, hne0, ↓reduceIte]
    have hneall' : BitVec.ofNat 11 (e + 1075).toNat ≠ -1#11 := by
      simpa [show (-1#11 : BitVec 11) = 2047#11 by decide] using hneall
    rw [if_neg hneall']
    congr 1
    · rw [append_one_ofNat52_toNat m hlo hhi]
    · rw [ofNat11_toNat_sub_bias e hb_lt hb_pos]

/- Rounding and normalization commute with an exponent increment, provided the
   value is in the normal range. -/

theorem round_succ (s : Sign) (m : Nat) (e : Int) (hm : 0 < m) (he : -52 ≤ e) :
    round Format.binary64 s m (e+1) = dbl (round Format.binary64 s m e) := by
  unfold round
  simp only [Format.targetExponent, totalExponent, Format.minExponent, Format.mantissaBits, Nat.reduceAdd]
  have hcap : 3 - 2 ^ (11 - 1) - (↑(53 : Nat) : Int) ≤ ↑m.log2 + 1 + e - (↑(53 : Nat) : Int) := by omega
  rw [Int.max_eq_left hcap]
  have hcap' : 3 - 2 ^ (11 - 1) - (↑(53 : Nat) : Int) ≤ ↑m.log2 + 1 + (e + 1) - (↑(53 : Nat) : Int) := by omega
  rw [Int.max_eq_left hcap']
  unfold decreaseExponent
  simp only
  have hsh : (e + 1 - (↑m.log2 + 1 + (e + 1) - ↑53)).toNat =
      (e - (↑m.log2 + 1 + e - ↑53)).toNat := by congr 1; omega
  simp [hsh]
  let sh := (e - (↑m.log2 + 1 + e - ↑53)).toNat
  let e' := e - (↑sh : Int)
  have hm' : 0 < m <<< sh := by
    rw [Nat.shiftLeft_eq]
    exact Nat.mul_pos hm (Nat.two_pow_pos sh)
  have hlog' : (m <<< sh).log2 = m.log2 + sh := log2_shiftLeft m sh hm
  have h1 : -1021 ≤ (m <<< sh).log2 + 1 + e' := by
    rw [hlog']
    dsimp [e', sh]
    omega
  have hlow : -1022 ≤ e' := by
    dsimp [e', sh]
    omega
  have hr := roundWA_succ s (m <<< sh) e' Accuracy.exact h1 hlow
  have heq1 : e + 1 - max (e - (↑m.log2 + 1 + e - 53)) 0 = e' + 1 := by
    dsimp [e', sh]
    omega
  have heq2 : e - max (e - (↑m.log2 + 1 + e - 53)) 0 = e' := by
    dsimp [e', sh]
    omega
  rw [heq1, heq2]
  exact hr

theorem normalize_succ (M : Int) (E : Int) (hE : -52 ≤ E) :
    normalize Format.binary64 M (E + 1) .positive = dbl (normalize Format.binary64 M E .positive) := by
  unfold normalize
  cases hcmp : compare M 0 with
  | lt =>
      have hneg : M < 0 := Int.compare_eq_lt.mp hcmp
      have hpos : 0 < (-M).toNat := by omega
      simp only [hcmp]
      rw [round_succ .negative (-M).toNat E hpos hE]
  | eq => simp [hcmp, dbl]
  | gt =>
      have hposM : 0 < M := Int.compare_eq_gt.mp hcmp
      have hpos : 0 < M.toNat := by omega
      simp only [hcmp]
      rw [round_succ .positive M.toNat E hpos hE]

/- Doubling a canonical finite value at the Float level. -/

theorem mul_two_unpack (x : Float) (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (hx : x.toModel.unpack = .finite s m e hm) (hlog : m.log2 = 52) (he : -52 ≤ e)
    (hfin : (2 * x).isFinite = true) :
    (2 * x).toModel.unpack = .finite s m (e + 1) hm := by
  have h2 : (2 : Float).toModel.unpack = .finite .positive 4503599627370496 (-51) (by decide) := by rfl
  have hs : .positive * s = s := by cases s <;> rfl
  change (Float.Model.pack (UnpackedFloat.mul Format.binary64 (2 : Float).toModel.unpack x.toModel.unpack)).unpack = _
  rw [hx, h2]
  simp only [UnpackedFloat.mul, hs]
  rw [Nat.mul_comm 4503599627370496 m]
  rw [show -51 + e = e + -51 by omega]
  rw [roundWA_mul_two s m e hm hlog he]
  apply unpack_pack_finite s m (e + 1) hm hlog
  · omega
  · have hfin' := hfin
    change (Float.Model.pack (UnpackedFloat.mul Format.binary64 (2 : Float).toModel.unpack x.toModel.unpack)).isFinite = true at hfin'
    rw [hx, h2] at hfin'
    simp only [UnpackedFloat.mul, hs] at hfin'
    rw [Nat.mul_comm 4503599627370496 m, show -51 + e = e + -51 by omega,
      roundWA_mul_two s m e hm hlog he] at hfin'
    exact hfin'

theorem unpack_pack_subnormal (s : Sign) (m : Nat) (hm : 0 < m)
    (hlog : m.log2 < 52)
    (hfin : (Float.Model.pack (.finite s m (-1074) hm)).isFinite = true) :
    (Float.Model.pack (.finite s m (-1074) hm)).unpack = .finite s m (-1074) hm := by
  have hhi : m < 4503599627370496 := by
    have hlt := lt_pow_succ_log2 m hm
    have hle : m.log2 + 1 ≤ 52 := by omega
    have hp : 2 ^ (m.log2 + 1) ≤ 2 ^ 52 := Nat.pow_le_pow_right (by omega : 0 < 2) hle
    exact Nat.lt_of_lt_of_le hlt hp
  have hlog' : m.log2 + 1 ≠ 1 + 52 := by omega
  unfold Float.Model.pack Float.Model.unpack
  simp only [UnpackedFloat.pack]
  simp only [hlog', Format.exponentBias, Format.mantissaBits]
  split
  · rename_i hov
    have hbad : (Float.Model.pack (.finite s m (-1074) hm)).isFinite = false := by
      unfold Float.Model.pack
      simp only [UnpackedFloat.pack, hlog']
      split
      · have hinf : UnpackedFloat.unpack Format.binary64 (packedInfinity Format.binary64 s) = .infinity s := by
          simpa [Float.Model.unpack, Float.Model.pack, UnpackedFloat.pack] using (unpack_pack_inf s)
        simp [Float.Model.unpack, Float.Model.isFinite, hinf, UnpackedFloat.isFinite]
      · rename_i hn
        exfalso
        apply hn
        simpa [Format.exponentBias] using hov
    rw [hbad] at hfin
    contradiction
  · rename_i hnov
    simp only [UnpackedFloat.unpack, unpackExponent_packComponents, unpackMantissa_packComponents,
      unpackSign_pc64, sign_of_toBitVec, ↓reduceIte]
    have hm0 : BitVec.ofNat 52 m ≠ 0#52 := by
      intro hz
      have ht := congrArg BitVec.toNat hz
      simp [BitVec.toNat_ofNat, Nat.mod_eq_of_lt hhi] at ht
      omega
    rw [if_neg hm0]
    simp only [Format.exponentBias, Format.mantissaBits]
    simp [hm0]
    exact hhi

theorem unpack_abs (x : Float) (hfin : x.isFinite = true) :
    (Float.abs x).toModel.unpack = x.toModel.unpack.abs := by
  change (Float.Model.pack x.toModel.unpack.abs).unpack = x.toModel.unpack.abs
  cases hx : x.toModel.unpack with
  | infinity s =>
      cases s <;> simpa [UnpackedFloat.abs] using (unpack_pack_inf .positive)
  | notANumber =>
      have hnan : x.isNaN = true := by simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, hx]
      have hnot := finite_not_nan hfin
      rw [hnan] at hnot
      contradiction
  | zero s =>
      cases s <;> simpa [UnpackedFloat.abs] using (unpack_pack_zero .positive)
  | finite s m e hm =>
      obtain ⟨hs, hfields⟩ := unpack_finite_fields x.toModel.toBits.toBitVec s m e hm hx
      rcases hfields with hsub | hnorm
      · rcases hsub with ⟨he0, hm0, hmeq, heeq⟩
        have hloglt : m.log2 < 52 := by
          have hle : m < 4503599627370496 := by
            rw [hmeq]
            exact (unpackMantissa (spec := Format.binary64) x.toModel.toBits.toBitVec).isLt
          by_cases hge52 : 52 ≤ m.log2
          · have hp : 2 ^ 52 ≤ 2 ^ m.log2 := Nat.pow_le_pow_right (by omega : 0 < 2) hge52
            have hp2 : 2 ^ m.log2 ≤ m := pow_log2_le m hm
            omega
          · omega
        have hmne : BitVec.ofNat 52 m ≠ 0#52 := by
          intro hz
          have ht := congrArg BitVec.toNat hz
          have hlt : m < 4503599627370496 := by
            rw [hmeq]
            exact (unpackMantissa (spec := Format.binary64) x.toModel.toBits.toBitVec).isLt
          simp only [BitVec.toNat_ofNat, BitVec.toNat_zero] at ht
          rw [Nat.mod_eq_of_lt hlt] at ht
          omega
        have hnb' : ¬ 2047 ≤ (-1074 + (1023 : Int) + 52).toNat := by decide
        have hlogc : ¬ m.log2 + 1 = Format.binary64.mantissaBits := by
          rw [show Format.binary64.mantissaBits = 53 by rfl]
          omega
        have hfinabs : (Float.Model.pack (.finite .positive m (-1074) hm)).isFinite = true := by
          unfold Float.Model.pack Float.Model.isFinite Float.Model.unpack
          simp only [UnpackedFloat.pack]
          simp [hnb', hlogc, show m.log2 ≠ 52 by omega, hmne, UnpackedFloat.isFinite, UnpackedFloat.unpack, Format.exponentBias, Format.mantissaBits]
        rw [show e = -1074 by omega]
        simp only [UnpackedFloat.abs]
        simpa using (unpack_pack_subnormal .positive m hm hloglt hfinabs)
      · rcases hnorm with ⟨he0, heall, hmeq, heeq⟩
        have hlog : m.log2 = 52 := by
          rw [hmeq]
          apply log2_eq_of_bounds
          · simpa using (Nat.left_le_or : 4503599627370496 ≤
              4503599627370496 ||| (unpackMantissa (spec := Format.binary64) x.toModel.toBits.toBitVec).toNat)
          · simpa using (1#1 ++ unpackMantissa (spec := Format.binary64) x.toModel.toBits.toBitVec).isLt
        have hb_lt : (e + 1075).toNat < 2047 := by
          have hb_eq : (e + 1075).toNat = (unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec).toNat := by omega
          rw [hb_eq]
          have hne : (unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec).toNat ≠ 2047 := by
            intro h
            apply heall
            apply BitVec.toNat_inj.mp
            simpa [BitVec.toNat_allOnes] using h
          have hlt : (unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec).toNat < 2048 :=
            (unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec).isLt
          omega
        have hnb : ¬ 2 ^ 11 ≤ (e + 1075).toNat + 1 := by omega
        have heneall : BitVec.ofNat 11 (e + 1075).toNat ≠ 2047#11 := by
          intro hz
          have ht := congrArg BitVec.toNat hz
          simp [BitVec.toNat_ofNat, Nat.mod_eq_of_lt (by omega : (e+1075).toNat < 2^11)] at ht
          omega
        have hb_eq : (e + 1075).toNat = (unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec).toNat := by omega
        have hbpos : 0 < (e + 1075).toNat := by
          rw [hb_eq]
          have hne : (unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec).toNat ≠ 0 := by
            intro hz
            apply he0
            apply BitVec.toNat_inj.mp
            simpa using hz
          omega
        have henezero : BitVec.ofNat 11 (e + 1075).toNat ≠ 0#11 := by
          intro hz
          have ht := congrArg BitVec.toNat hz
          simp [BitVec.toNat_ofNat, Nat.mod_eq_of_lt (by omega : (e+1075).toNat < 2^11)] at ht
          omega
        have hnb' : ¬ 2047 ≤ (e + (1023 : Int) + 52).toNat := by omega
        have hfinabs : (Float.Model.pack (.finite .positive m e hm)).isFinite = true := by
          unfold Float.Model.pack Float.Model.isFinite Float.Model.unpack
          simp only [UnpackedFloat.pack, hlog]
          simp [hnb', show (e + (1023 : Int) + 52) = e + 1075 by omega, show ¬ 2047 ≤ (e + 1075).toNat by omega, heneall, henezero, UnpackedFloat.isFinite, UnpackedFloat.unpack, Format.exponentBias, Format.mantissaBits]
        simp only [UnpackedFloat.abs]
        simpa using (unpack_pack_finite .positive m e hm hlog (by omega) hfinabs)

theorem le_one_imp_exp (m : Nat) (e : Int) (hm : 0 < m)
    (h : UnpackedFloat.le (.finite .positive 4503599627370496 (-52) (by decide)) (.finite .positive m e hm) = true) :
    -52 ≤ e := by
  simp only [UnpackedFloat.le, UnpackedFloat.compare] at h
  cases hcmp : compare (-52 : Int) e with
  | lt => have hlt : -52 < e := Int.compare_eq_lt.mp hcmp; omega
  | eq => have heq : -52 = e := Int.compare_eq_eq.mp hcmp; omega
  | gt => simp [hcmp] at h

theorem unpack_ge_one (x : Float) (hfin : x.isFinite = true) (hge : 1 ≤ Float.abs x) :
    ∃ s m e hm, x.toModel.unpack = .finite s m e hm ∧ m.log2 = 52 ∧ -52 ≤ e := by
  have hle : Float.Model.le (1 : Float).toModel (Float.abs x).toModel = true := hge
  unfold Float.Model.le at hle
  rw [unpack_abs x hfin] at hle
  have h1 : (1 : Float).toModel.unpack = .finite .positive 4503599627370496 (-52) (by decide) := by rfl
  rw [h1] at hle
  cases hx : x.toModel.unpack with
  | infinity s =>
      have hi : x.isInf = true := by simp [Float.isInf, Float.Model.isInf, UnpackedFloat.isInf, hx]
      have hn := finite_not_inf hfin
      rw [hi] at hn
      contradiction
  | notANumber =>
      have hi : x.isNaN = true := by simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, hx]
      have hn := finite_not_nan hfin
      rw [hi] at hn
      contradiction
  | zero s =>
      rw [hx, UnpackedFloat.abs] at hle
      simp [UnpackedFloat.le, UnpackedFloat.compare] at hle
  | finite s m e hm =>
      rw [hx, UnpackedFloat.abs] at hle
      have helow : -52 ≤ e := le_one_imp_exp m e hm hle
      obtain ⟨hs, hfields⟩ := unpack_finite_fields x.toModel.toBits.toBitVec s m e hm hx
      rcases hfields with hsub | hnorm
      · rcases hsub with ⟨he0, hm0, hmeq, heeq⟩
        omega
      · rcases hnorm with ⟨he0, heall, hmeq, heeq⟩
        have hlog : m.log2 = 52 := by
          rw [hmeq]
          apply log2_eq_of_bounds
          · simpa using (Nat.left_le_or : 4503599627370496 ≤
              4503599627370496 ||| (unpackMantissa (spec := Format.binary64) x.toModel.toBits.toBitVec).toNat)
          · simpa using (1#1 ++ unpackMantissa (spec := Format.binary64) x.toModel.toBits.toBitVec).isLt
        exact ⟨s, m, e, hm, rfl, hlog, helow⟩


end TinylambOpt.ResearchProofs.JitKernels.FloatingReductionScale
