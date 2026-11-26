import Std
import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
import TinylambOpt.ResearchProofs.JitKernels.Common_FloatingReduction

/-!
Certificate for `CanonicalZeroHashJoin.Goal`: canonicalizing IEEE-equal floats before
hashing gives identical bits. Equal nonzero unpacked representations determine equal
`Float.Model` values, and both signs of zero are canonicalized to positive zero. This
allows a JIT hash join to replace signed-zero inputs with `+0` without changing the
hash key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace Nov26.Proof_FloatingReduction_CanonicalZeroHashJoin

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.CanonicalZeroHashJoin
set_option linter.unusedSimpArgs false
open Float.Model
open Float.Model.UnpackedFloat
open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionCommon

theorem bv_eq_of_unpack_fields {b1 b2 : BitVec 64}
  (hs : unpackSign (spec := Format.binary64) b1 = unpackSign (spec := Format.binary64) b2)
  (he : unpackExponent (spec := Format.binary64) b1 = unpackExponent (spec := Format.binary64) b2)
  (hm : unpackMantissa (spec := Format.binary64) b1 = unpackMantissa (spec := Format.binary64) b2) : b1 = b2 := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  by_cases h0 : i < 52
  · have hm_i := congrArg (fun x : BitVec 52 => x.getLsbD i) hm
    simpa [unpackMantissa, h0] using hm_i
  · by_cases h1 : i < 63
    · have he_i := congrArg (fun x : BitVec 11 => x.getLsbD (i-52)) he
      have hidx : i - 52 < 11 := by omega
      have hEq : 52 + (i - 52) = i := by omega
      simpa [unpackExponent, hidx, hEq] using he_i
    · have hs_i := congrArg (fun x : BitVec 1 => x.getLsbD (i-63)) hs
      have hidx : i - 63 < 1 := by omega
      have hEq : 63 + (i - 63) = i := by omega
      simpa [unpackSign, hidx, hEq] using hs_i

theorem pack_unpack_valid_bv (b : BitVec 64) (hv : Format.Valid Format.binary64 b) :
    UnpackedFloat.pack Format.binary64 (UnpackedFloat.unpack Format.binary64 b) = b := by
  let E : BitVec 11 := unpackExponent (spec := Format.binary64) b
  let M : BitVec 52 := unpackMantissa (spec := Format.binary64) b
  have hEdef : unpackExponent (spec := Format.binary64) b = E := rfl
  have hMdef : unpackMantissa (spec := Format.binary64) b = M := rfl
  by_cases hEall : E = (-1#11)
  · by_cases hM0 : M = (0#52)
    · have hs : unpackSign (spec := Format.binary64) (packedInfinity Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b))) =
          unpackSign (spec := Format.binary64) b := by
        simp [packedInfinity, unpackSign_pc64, signBit_ofBitVec]
      have he : unpackExponent (spec := Format.binary64) (packedInfinity Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b))) =
          unpackExponent (spec := Format.binary64) b := by
        simpa [packedInfinity, unpackExponent_packComponents, E] using hEall.symm
      have hm : unpackMantissa (spec := Format.binary64) (packedInfinity Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b))) =
          unpackMantissa (spec := Format.binary64) b := by
        simpa [packedInfinity, unpackMantissa_packComponents, M] using hM0.symm
      have hb : packedInfinity Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b)) = b :=
        bv_eq_of_unpack_fields hs he hm
      rw [← hb]
      have hsign : Sign.ofBitVec (unpackSign (spec := Format.binary64) (packedInfinity Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b)))) =
          Sign.ofBitVec (unpackSign (spec := Format.binary64) b) := congrArg Sign.ofBitVec hs
      simp [UnpackedFloat.unpack, UnpackedFloat.pack, hEdef, hMdef, hEall, hM0, Format.binary64, packedInfinity,
        unpackExponent_packComponents, unpackMantissa_packComponents, unpackSign_pc64, sign_of_toBitVec, hsign]
    · have hb : b = packedNaN Format.binary64 := hv.eq_packedNaN (by simpa [E] using hEall) (by simpa [M] using hM0)
      rw [hb]
      rw [show UnpackedFloat.unpack Format.binary64 (packedNaN Format.binary64) = .notANumber by
        simp [packedNaN, UnpackedFloat.unpack, unpackExponent_packComponents, unpackMantissa_packComponents]]
      rfl
  · by_cases hE0 : E = (0#11)
    · by_cases hM0 : M = (0#52)
      · have hs : unpackSign (spec := Format.binary64) (packedZero Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b))) =
            unpackSign (spec := Format.binary64) b := by
          simp [packedZero, unpackSign_pc64, signBit_ofBitVec]
        have he : unpackExponent (spec := Format.binary64) (packedZero Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b))) =
            unpackExponent (spec := Format.binary64) b := by
          simpa [packedZero, unpackExponent_packComponents, E] using hE0.symm
        have hm : unpackMantissa (spec := Format.binary64) (packedZero Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b))) =
            unpackMantissa (spec := Format.binary64) b := by
          simpa [packedZero, unpackMantissa_packComponents, M] using hM0.symm
        have hb : packedZero Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b)) = b :=
          bv_eq_of_unpack_fields hs he hm
        rw [← hb]
        have hsign : Sign.ofBitVec (unpackSign (spec := Format.binary64) (packedZero Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) b)))) =
            Sign.ofBitVec (unpackSign (spec := Format.binary64) b) := congrArg Sign.ofBitVec hs
        simp [UnpackedFloat.unpack, UnpackedFloat.pack, hEdef, hMdef, hEall, hE0, hM0, Format.binary64, packedZero,
          unpackExponent_packComponents, unpackMantissa_packComponents, unpackSign_pc64, sign_of_toBitVec, hsign]
      · have hlog2 : M.toNat.log2 ≠ 52 := by
          intro hl
          have hlt : M.toNat < 2^52 := BitVec.toNat_lt_twoPow_of_le (by omega : 52 ≤ 52)
          have hmn : M.toNat ≠ 0 := by
            intro hz
            apply hM0
            apply BitVec.eq_of_toNat_eq
            simpa using hz
          have hlt' : M.toNat.log2 < 52 := (Nat.log2_lt hmn).mpr hlt
          omega
        have he0 : unpackExponent (spec := Format.binary64) b = (0#11) := by simpa [E] using hE0
        simp [E, M, UnpackedFloat.unpack, UnpackedFloat.pack, hEall, he0, hM0, hlog2,
          Format.exponentBias, Format.mantissaBits, Format.binary64]
        apply bv_eq_of_unpack_fields
        · simp [unpackSign_pc64, signBit_ofBitVec]
        · simpa [unpackExponent_packComponents] using he0.symm
        · simp [unpackMantissa_packComponents]
    · have hElt2048 : E.toNat < 2048 := BitVec.toNat_lt_twoPow_of_le (by omega : 11 ≤ 11)
      have hEne2047 : E.toNat ≠ 2047 := by
        intro heq
        apply hEall
        apply BitVec.eq_of_toNat_eq
        simp [heq]
      have hEpos : 0 < E.toNat := by
        by_cases hEz : E.toNat = 0
        · exfalso
          apply hE0
          apply BitVec.eq_of_toNat_eq
          simp [hEz]
        · omega
      have hbiased : (-(↑Format.binary64.exponentBias + (52 : Int)) + (E.toNat : Int) + ↑Format.binary64.exponentBias + 52).toNat = E.toNat := by
        simp [Format.exponentBias]
        omega
      have hmval : ((1#1 : BitVec 1) ++ M).toNat = 2^52 + M.toNat := by
        rw [BitVec.toNat_append_eq_mul_add]
        simp
      have hmlog2 : ((1#1 : BitVec 1) ++ M).toNat.log2 = 52 := by
        rw [hmval]
        apply (Nat.log2_eq_iff (by omega)).mpr
        constructor
        · omega
        · have hlt : M.toNat < 2^52 := BitVec.toNat_lt_twoPow_of_le (by omega : 52 ≤ 52)
          omega
      have hmant : BitVec.ofNat 52 (((1#1 : BitVec 1) ++ M).toNat) = M := by
        apply BitVec.eq_of_toNat_eq
        rw [hmval]
        have hlt : M.toNat < 2^52 := BitVec.toNat_lt_twoPow_of_le (by omega : 52 ≤ 52)
        simp [Nat.add_mod, Nat.mod_eq_of_lt hlt]
      have hEofNat : BitVec.ofNat 11 E.toNat = E := by
        apply BitVec.eq_of_toNat_eq
        simp [Nat.mod_eq_of_lt E.isLt]
      have hEall' : unpackExponent (spec := Format.binary64) b ≠ (-1#11) := by simpa [E] using hEall
      have hEall'' : unpackExponent (spec := Format.binary64) b ≠ (2047#11) := hEall'
      have hE2047bits : E ≠ (2047#11) := by
        intro h
        apply hEne2047
        simpa using congrArg BitVec.toNat h
      have hEne2047raw : (unpackExponent (spec := Format.binary64) b).toNat ≠ 2047 := by
        simpa [hEdef] using hEne2047
      have hE0' : unpackExponent (spec := Format.binary64) b ≠ (0#11) := by simpa [E] using hE0
      have hbiasedNum : ((E.toNat : Int) - 1075 + 1023 + 52).toNat = E.toNat := by
        simp [Format.exponentBias] at hbiased ⊢
        omega
      have hnotE : ¬ 2047 ≤ ((E.toNat : Int) - 1075 + 1023 + 52).toNat := by
        rw [hbiasedNum]
        omega
      have hnotE' : ¬ 2047 ≤ E.toNat := by omega
      have hlogM : (4503599627370496 ||| M.toNat).log2 = 52 := by
        simpa [BitVec.toNat_append] using hmlog2
      simp [UnpackedFloat.unpack, UnpackedFloat.pack, hEdef, hMdef, hEall, hEall', hEall'', hE0, hE0',
        hE2047bits, hbiased, hbiasedNum, hnotE, hnotE', hlogM, Format.exponentBias, Format.mantissaBits, Format.binary64]
      apply bv_eq_of_unpack_fields
      · simp [unpackSign_pc64, signBit_ofBitVec]
      · simpa [unpackExponent_packComponents, hEofNat, hbiasedNum]
      · simpa [unpackMantissa_packComponents, hmant, hMdef]

theorem model_ofBits_toBits (m : Float.Model) : Float.Model.ofBits m.toBits = m := by
  cases m with
  | mk bits valid =>
      rw [Float.Model.mk.injEq]
      change UInt64.ofBitVec (UnpackedFloat.pack Format.binary64 (UnpackedFloat.unpack Format.binary64 bits.toBitVec)) = bits
      have hp := pack_unpack_valid_bv bits.toBitVec (by simpa using valid)
      rw [hp]

set_option linter.unusedSimpArgs false
open Float.Model
open Float.Model.UnpackedFloat

theorem model_eq_of_unpack_eq (m1 m2 : Float.Model) (h : m1.unpack = m2.unpack) : m1 = m2 := by
  rw [← model_ofBits_toBits m1, ← model_ofBits_toBits m2]
  change Float.Model.pack m1.unpack = Float.Model.pack m2.unpack
  rw [h]

theorem unpack_eq_of_beq_of_unpack_ne_zero {a b : Float}
    (h : a == b) (haNZ : a.toModel.unpack ≠ .zero .positive)
    (haNZ' : a.toModel.unpack ≠ .zero .negative) :
    a.toModel.unpack = b.toModel.unpack := by
  change a.toModel.unpack.beq b.toModel.unpack = true at h
  cases hxa : a.toModel.unpack with
  | notANumber =>
      simp [UnpackedFloat.beq, UnpackedFloat.compare, hxa] at h
  | infinity sa =>
      unfold UnpackedFloat.beq at h
      unfold UnpackedFloat.compare at h
      cases hxb : b.toModel.unpack with
      | notANumber => rw [hxa, hxb] at h; simp at h
      | infinity sb =>
          rw [hxa, hxb] at h
          cases sa <;> cases sb <;> simp at h ⊢
      | zero sb => rw [hxa, hxb] at h; cases sa <;> simp at h
      | finite sb mb eb hmb => rw [hxa, hxb] at h; cases sa <;> simp at h
  | zero sa =>
      rw [hxa] at haNZ haNZ'
      cases sa <;> simp at haNZ haNZ'
  | finite sa ma ea hma =>
      unfold UnpackedFloat.beq at h
      unfold UnpackedFloat.compare at h
      cases hxb : b.toModel.unpack with
      | notANumber => rw [hxa, hxb] at h; simp at h
      | infinity sb => rw [hxa, hxb] at h; cases sb <;> simp at h
      | zero sb => rw [hxa, hxb] at h; cases sa <;> simp at h
      | finite sb mb eb hmb =>
          rw [hxa, hxb] at h
          cases sa <;> cases sb <;> simp at h
          · rcases h with ⟨he, hm⟩
            subst he
            subst hm
            rfl
          · rcases h with ⟨he, hm⟩
            subst he
            subst hm
            rfl

theorem zero_unpack : (0 : Float).toModel.unpack = .zero .positive := by
  change UnpackedFloat.unpack Format.binary64 (0#64) = .zero .positive
  have hE : unpackExponent (spec := Format.binary64) (0#64) = (0#11) := by simp [unpackExponent]
  have hM : unpackMantissa (spec := Format.binary64) (0#64) = (0#52) := by simp [unpackMantissa]
  have hS : unpackSign (spec := Format.binary64) (0#64) = (0#1) := by simp [unpackSign]
  simp [UnpackedFloat.unpack, hE, hM, hS, Sign.ofBitVec]

theorem unpack_zero_of_beq_zero {a : Float} (h : a == 0) :
    a.toModel.unpack = .zero .positive ∨ a.toModel.unpack = .zero .negative := by
  change a.toModel.unpack.beq (0 : Float).toModel.unpack = true at h
  rw [zero_unpack] at h
  cases hxa : a.toModel.unpack with
  | notANumber => rw [hxa] at h; simp [UnpackedFloat.beq, UnpackedFloat.compare] at h
  | infinity sa => rw [hxa] at h; unfold UnpackedFloat.beq at h; unfold UnpackedFloat.compare at h; cases sa <;> simp at h
  | zero sa => cases sa <;> simp
  | finite sa ma ea hma => rw [hxa] at h; unfold UnpackedFloat.beq at h; unfold UnpackedFloat.compare at h; cases sa <;> simp at h

theorem beq_zero_of_unpack_zero {a : Float}
    (h : a.toModel.unpack = .zero .positive ∨ a.toModel.unpack = .zero .negative) : a == 0 := by
  change a.toModel.unpack.beq (0 : Float).toModel.unpack = true
  rw [zero_unpack]
  rcases h with h | h <;> rw [h] <;> simp [UnpackedFloat.beq, UnpackedFloat.compare]

theorem beq_zero_of_beq_of_beq_zero_left {a b : Float} (hab : a == b) (ha : a == 0) : b == 0 := by
  change a.toModel.unpack.beq b.toModel.unpack = true at hab
  change b.toModel.unpack.beq (0 : Float).toModel.unpack = true
  rw [zero_unpack]
  have haz := unpack_zero_of_beq_zero ha
  rcases haz with haz | haz
  · rw [haz] at hab
    cases hxb : b.toModel.unpack with
    | notANumber => rw [hxb] at hab; simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
    | infinity sb => rw [hxb] at hab; unfold UnpackedFloat.beq at hab; unfold UnpackedFloat.compare at hab; cases sb <;> simp at hab
    | zero sb => cases sb <;> simp [UnpackedFloat.beq, UnpackedFloat.compare]
    | finite sb mb eb hmb => rw [hxb] at hab; unfold UnpackedFloat.beq at hab; unfold UnpackedFloat.compare at hab; cases sb <;> simp at hab
  · rw [haz] at hab
    cases hxb : b.toModel.unpack with
    | notANumber => rw [hxb] at hab; simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
    | infinity sb => rw [hxb] at hab; unfold UnpackedFloat.beq at hab; unfold UnpackedFloat.compare at hab; cases sb <;> simp at hab
    | zero sb => cases sb <;> simp [UnpackedFloat.beq, UnpackedFloat.compare]
    | finite sb mb eb hmb => rw [hxb] at hab; unfold UnpackedFloat.beq at hab; unfold UnpackedFloat.compare at hab; cases sb <;> simp at hab

theorem beq_zero_of_beq_of_beq_zero_right {a b : Float} (hab : a == b) (hb : b == 0) : a == 0 := by
  change a.toModel.unpack.beq b.toModel.unpack = true at hab
  change a.toModel.unpack.beq (0 : Float).toModel.unpack = true
  rw [zero_unpack]
  have hbz := unpack_zero_of_beq_zero hb
  rcases hbz with hbz | hbz
  · rw [hbz] at hab
    cases hxa : a.toModel.unpack with
    | notANumber => rw [hxa] at hab; simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
    | infinity sa => rw [hxa] at hab; unfold UnpackedFloat.beq at hab; unfold UnpackedFloat.compare at hab; cases sa <;> simp at hab
    | zero sa => cases sa <;> simp [UnpackedFloat.beq, UnpackedFloat.compare]
    | finite sa ma ea hma => rw [hxa] at hab; unfold UnpackedFloat.beq at hab; unfold UnpackedFloat.compare at hab; cases sa <;> simp at hab
  · rw [hbz] at hab
    cases hxa : a.toModel.unpack with
    | notANumber => rw [hxa] at hab; simp [UnpackedFloat.beq, UnpackedFloat.compare] at hab
    | infinity sa => rw [hxa] at hab; unfold UnpackedFloat.beq at hab; unfold UnpackedFloat.compare at hab; cases sa <;> simp at hab
    | zero sa => cases sa <;> simp [UnpackedFloat.beq, UnpackedFloat.compare]
    | finite sa ma ea hma => rw [hxa] at hab; unfold UnpackedFloat.beq at hab; unfold UnpackedFloat.compare at hab; cases sa <;> simp at hab

theorem sound : Goal := by
  intro a b hab
  by_cases ha : a == 0
  · have hb : b == 0 := beq_zero_of_beq_of_beq_zero_left hab ha
    simp [ha, hb]
  · have hb : ¬ (b == 0) := by
      intro hb
      exact ha (beq_zero_of_beq_of_beq_zero_right hab hb)
    have haNZp : a.toModel.unpack ≠ .zero .positive := by
      intro hz
      exact ha (beq_zero_of_unpack_zero (Or.inl hz))
    have haNZn : a.toModel.unpack ≠ .zero .negative := by
      intro hz
      exact ha (beq_zero_of_unpack_zero (Or.inr hz))
    have hup : a.toModel.unpack = b.toModel.unpack :=
      unpack_eq_of_beq_of_unpack_ne_zero hab haNZp haNZn
    have hm : a.toModel = b.toModel := model_eq_of_unpack_eq a.toModel b.toModel hup
    simp [ha, hb, TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.bits]
    exact congrArg Float.Model.toBits hm
end Nov26.Proof_FloatingReduction_CanonicalZeroHashJoin
