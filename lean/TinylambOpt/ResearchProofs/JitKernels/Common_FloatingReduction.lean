import Std
import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-! Shared facts about Lean's IEEE-754 `Float` model used by the floating-reduction
JIT-kernel goals. `Float` operations have a logical model (`Float.Model`) which
unpacks a binary64 bit pattern into an `UnpackedFloat`, applies the operation there,
and packs the result back. The lemmas below relate the observable
`isFinite`/`isInf`/`isNaN` flags and addition of a finite input to that unpacked
representation. They let an execution keep the sticky overflow flag of a
finite-input reduction without replaying the prefix to detect a NaN. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReductionCommon

open Float.Model
open Float.Model.UnpackedFloat
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/- Bit-level facts about field extraction from packed binary64 patterns. -/

theorem sign_of_toBitVec (s : Sign) : Sign.ofBitVec s.toBitVec = s := by
  cases s <;> rfl

theorem signBit_ofBitVec (b : BitVec 1) : (Sign.ofBitVec b).toBitVec = b := by
  simp [Sign.ofBitVec, Sign.toBitVec]
  bv_decide

theorem unpackSign_pc64 (s : Sign) (e : BitVec 11) (m : BitVec 52) :
    unpackSign (spec := Format.binary64) (packComponents Format.binary64 s e m) = s.toBitVec := by
  cases s <;> simp only [unpackSign, packComponents, Sign.toBitVec] <;> bv_decide

theorem unpackExp_append (s : Sign) (e : BitVec 11) (m : BitVec 52) :
    unpackExponent (spec := Format.binary64) (s.toBitVec ++ e ++ m) = e := by
  cases s <;> simp only [unpackExponent, Sign.toBitVec] <;> bv_decide

theorem unpackMan_append (s : Sign) (e : BitVec 11) (m : BitVec 52) :
    unpackMantissa (spec := Format.binary64) (s.toBitVec ++ e ++ m) = m := by
  cases s <;> simp only [unpackMantissa, Sign.toBitVec] <;> bv_decide

theorem unpackSign_append (s : Sign) (e : BitVec 11) (m : BitVec 52) :
    unpackSign (spec := Format.binary64) (s.toBitVec ++ e ++ m) = s.toBitVec := by
  cases s <;> simp only [unpackSign, Sign.toBitVec] <;> bv_decide

theorem ofNat11_ne_2047 {n : Nat} (h : n < 2047) : BitVec.ofNat 11 n ≠ 2047#11 := by
  intro heq
  have ht := congrArg BitVec.toNat heq
  simp [BitVec.toNat_ofNat, Nat.mod_eq_of_lt (by omega : n < 2 ^ 11)] at ht
  omega

/- Unpacking a special packed value recovers it exactly. -/

theorem unpack_pack_inf (s : Sign) : (Float.Model.pack (.infinity s)).unpack = .infinity s := by
  simp [Float.Model.unpack, Float.Model.pack, UnpackedFloat.unpack, UnpackedFloat.pack,
    packedInfinity, unpackSign_pc64, sign_of_toBitVec]

theorem unpack_pack_zero (s : Sign) : (Float.Model.pack (.zero s)).unpack = .zero s := by
  simp [Float.Model.unpack, Float.Model.pack, UnpackedFloat.unpack, UnpackedFloat.pack,
    packedZero, unpackSign_pc64, sign_of_toBitVec]

theorem isNaN_pack_inf (s : Sign) : (Float.Model.pack (.infinity s)).isNaN = false := by
  simp [Float.Model.isNaN, UnpackedFloat.isNaN, unpack_pack_inf]

theorem isNaN_pack_zero (s : Sign) : (Float.Model.pack (.zero s)).isNaN = false := by
  simp [Float.Model.isNaN, UnpackedFloat.isNaN, unpack_pack_zero]

theorem isNaN_pack_finite (s : Sign) (m : Nat) (e : Int) (hm : 0 < m) :
    (Float.Model.pack (.finite s m e hm)).isNaN = false := by
  simp only [Float.Model.isNaN, Float.Model.pack, Float.Model.unpack, UnpackedFloat.pack]
  split
  · simp [UnpackedFloat.unpack, UnpackedFloat.isNaN, packedInfinity, unpackSign_pc64, signBit_ofBitVec]
  · split
    · have hlt : (e + ↑Format.binary64.exponentBias + 52).toNat < 2047 := by omega
      by_cases h0 : BitVec.ofNat 11 (e + ↑Format.binary64.exponentBias + 52).toNat = 0#11 <;>
        by_cases hm0 : BitVec.ofNat 52 m = 0#52 <;>
        simp [UnpackedFloat.unpack, UnpackedFloat.isNaN, packComponents, unpackSign_pc64,
          signBit_ofBitVec, unpackExp_append, unpackMan_append, unpackSign_append,
          sign_of_toBitVec, ofNat11_ne_2047 hlt, h0, hm0]
    · by_cases h0 : (0#11 : BitVec 11) = 0#11 <;>
        by_cases hm0 : BitVec.ofNat 52 m = 0#52 <;>
        simp [UnpackedFloat.unpack, UnpackedFloat.isNaN, packComponents, unpackSign_pc64,
          signBit_ofBitVec, unpackExp_append, unpackMan_append, unpackSign_append,
          sign_of_toBitVec, h0, hm0]

/- `normalize` (used by finite + finite addition and by multiplication) always
returns zero or a finite value, so packing it never yields NaN. -/

theorem normalize_isNaN_false (s : Sign) (m e : Int) :
    (normalize Format.binary64 m e s).isNaN = false := by
  unfold normalize
  cases hcmp : compare m 0 <;> simp [hcmp, round, roundWithAccuracy] <;>
    (try split) <;> simp [UnpackedFloat.isNaN]

theorem normalize_isInf_false (s : Sign) (m e : Int) :
    (normalize Format.binary64 m e s).isInf = false := by
  unfold normalize
  cases hcmp : compare m 0 <;> simp [hcmp, round, roundWithAccuracy] <;>
    (try split) <;> simp [UnpackedFloat.isInf]

theorem isNaN_pack_normalize (s : Sign) (m e : Int) :
    (Float.Model.pack (normalize Format.binary64 m e s)).isNaN = false := by
  cases hu : normalize Format.binary64 m e s with
  | notANumber =>
      have h := normalize_isNaN_false s m e
      simp [hu, UnpackedFloat.isNaN] at h
  | infinity s2 =>
      have h := normalize_isInf_false s m e
      simp [hu, UnpackedFloat.isInf] at h
  | zero s2 => simpa [hu] using isNaN_pack_zero s2
  | finite s2 m2 e2 hm2 => simpa [hu] using isNaN_pack_finite s2 m2 e2 hm2

/- Status of a `Float` is determined by its unpacked form. -/

theorem finite_not_inf {x : Float} (h : x.isFinite = true) : x.isInf = false := by
  simp only [Float.isFinite, Float.Model.isFinite, UnpackedFloat.isFinite] at h
  cases hx : x.toModel.unpack <;>
    simp [Float.isInf, Float.Model.isInf, UnpackedFloat.isInf, hx] at h ⊢

theorem finite_not_nan {x : Float} (h : x.isFinite = true) : x.isNaN = false := by
  simp only [Float.isFinite, Float.Model.isFinite, UnpackedFloat.isFinite] at h
  cases hx : x.toModel.unpack <;>
    simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, hx] at h ⊢

theorem inf_not_nan {x : Float} (h : x.isInf = true) : x.isNaN = false := by
  simp only [Float.isInf, Float.Model.isInf, UnpackedFloat.isInf] at h
  cases hx : x.toModel.unpack <;>
    simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, hx] at h ⊢

/- Once an accumulator has overflowed to infinity, adding a finite input keeps it
infinite (IEEE-754 `inf ± finite = inf`), so the sticky flag cannot turn off. -/

theorem add_unpack_inf_of_inf_of_finite (x y : Float) (s : Sign)
    (hx : x.toModel.unpack = .infinity s) (hy : y.toModel.unpack.isFinite = true) :
    (x + y).toModel.unpack = .infinity s := by
  cases hyc : y.toModel.unpack with
  | infinity s2 => simp [hyc, UnpackedFloat.isFinite] at hy
  | notANumber => simp [hyc, UnpackedFloat.isFinite] at hy
  | zero s2 =>
      change (Float.Model.add x.toModel y.toModel).unpack = .infinity s
      simp only [Float.Model.add]
      rw [hx, hyc]
      simp only [UnpackedFloat.add]
      rw [unpack_pack_inf s]
  | finite s2 m e hm =>
      change (Float.Model.add x.toModel y.toModel).unpack = .infinity s
      simp only [Float.Model.add]
      rw [hx, hyc]
      simp only [UnpackedFloat.add]
      rw [unpack_pack_inf s]

theorem add_inf_of_inf_of_finite (x y : Float) (s : Sign)
    (hx : x.toModel.unpack = .infinity s) (hy : y.toModel.unpack.isFinite = true) :
    (x + y).isInf = true := by
  cases hyc : y.toModel.unpack with
  | infinity s2 => simp [hyc, UnpackedFloat.isFinite] at hy
  | notANumber => simp [hyc, UnpackedFloat.isFinite] at hy
  | zero s2 =>
      change (Float.Model.add x.toModel y.toModel).isInf = true
      simp only [Float.Model.add]
      rw [hx, hyc]
      simp only [UnpackedFloat.add]
      change (Float.Model.pack (.infinity s)).unpack.isInf = true
      rw [unpack_pack_inf s]
      simp [UnpackedFloat.isInf]
  | finite s2 m e hm =>
      change (Float.Model.add x.toModel y.toModel).isInf = true
      simp only [Float.Model.add]
      rw [hx, hyc]
      simp only [UnpackedFloat.add]
      change (Float.Model.pack (.infinity s)).unpack.isInf = true
      rw [unpack_pack_inf s]
      simp [UnpackedFloat.isInf]

/- Adding a finite input to a non-NaN accumulator can never create a NaN: the
unpacked adder returns only zero/finite for finite operands and infinity for an
infinite operand, and packing those never yields NaN. -/

theorem add_not_nan_of_not_nan_of_finite (x y : Float) (hx : x.isNaN = false)
    (hy : y.toModel.unpack.isFinite = true) : (x + y).isNaN = false := by
  cases hxc : x.toModel.unpack with
  | notANumber => simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, hxc] at hx
  | infinity sx =>
      cases hyc : y.toModel.unpack with
      | infinity s2 => simp [hyc, UnpackedFloat.isFinite] at hy
      | notANumber => simp [hyc, UnpackedFloat.isFinite] at hy
      | zero s2 =>
          change (Float.Model.add x.toModel y.toModel).isNaN = false
          simp only [Float.Model.add]
          rw [hxc, hyc]
          simp only [UnpackedFloat.add]
          simpa using isNaN_pack_inf sx
      | finite s2 m e hm =>
          change (Float.Model.add x.toModel y.toModel).isNaN = false
          simp only [Float.Model.add]
          rw [hxc, hyc]
          simp only [UnpackedFloat.add]
          simpa using isNaN_pack_inf sx
  | zero sx =>
      cases hyc : y.toModel.unpack with
      | infinity s2 => simp [hyc, UnpackedFloat.isFinite] at hy
      | notANumber => simp [hyc, UnpackedFloat.isFinite] at hy
      | zero s2 =>
          change (Float.Model.add x.toModel y.toModel).isNaN = false
          simp only [Float.Model.add]
          rw [hxc, hyc]
          simp only [UnpackedFloat.add]
          cases sx <;> cases s2 <;> simp [UnpackedFloat.add] <;>
            exact isNaN_pack_zero _
      | finite s2 m e hm =>
          change (Float.Model.add x.toModel y.toModel).isNaN = false
          simp only [Float.Model.add]
          rw [hxc, hyc]
          simp only [UnpackedFloat.add]
          simpa using isNaN_pack_finite s2 m e hm
  | finite sx mx ex hmx =>
      cases hyc : y.toModel.unpack with
      | infinity s2 => simp [hyc, UnpackedFloat.isFinite] at hy
      | notANumber => simp [hyc, UnpackedFloat.isFinite] at hy
      | zero s2 =>
          change (Float.Model.add x.toModel y.toModel).isNaN = false
          simp only [Float.Model.add]
          rw [hxc, hyc]
          simp only [UnpackedFloat.add]
          simpa using isNaN_pack_finite sx mx ex hmx
      | finite s2 m e hm =>
          change (Float.Model.add x.toModel y.toModel).isNaN = false
          simp only [Float.Model.add]
          rw [hxc, hyc]
          simp only [UnpackedFloat.add]
          simpa [UnpackedFloat.add] using isNaN_pack_normalize Sign.positive
            (sx.apply (decreaseExponent mx ex (min ex e)).fst +
              s2.apply (decreaseExponent m e (min ex e)).fst) (min ex e)

end TinylambOpt.ResearchProofs.JitKernels.FloatingReductionCommon
