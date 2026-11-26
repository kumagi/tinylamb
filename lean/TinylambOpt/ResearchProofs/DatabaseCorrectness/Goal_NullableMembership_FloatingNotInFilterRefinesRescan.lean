import TinylambOpt.ResearchGoals.DatabaseCorrectness.NullableMembership
import TinylambOpt.ResearchProofs.JitKernels.Common_FloatingReduction

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_NullableMembership_FloatingNotInFilterRefinesRescan

open TinylambOpt.ResearchGoals.DatabaseCorrectness.NullableMembership
open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionCommon
open Float.Model
open Float.Model.UnpackedFloat

theorem bitvec64_decompose (b : BitVec 64) :
    b = b.extractLsb' 63 1 ++ b.extractLsb' 52 11 ++ b.extractLsb' 0 52 := by
  ext i hi
  rw [BitVec.getElem_append (show i < 12 + 52 by omega)]
  by_cases h52 : i < 52
  · simp [h52, BitVec.getElem_extractLsb']
    rw [BitVec.getLsbD_eq_getElem (show i < 64 by omega)]
  · simp [h52]
    rw [BitVec.getElem_append (show i - 52 < 1 + 11 by omega)]
    by_cases h11 : i - 52 < 11
    · simp [h11, BitVec.getElem_extractLsb']
      rw [BitVec.getLsbD_eq_getElem (show 52 + (i - 52) < 64 by omega)]
      congr 1 <;> omega
    · simp [h11, BitVec.getElem_extractLsb']
      rw [BitVec.getLsbD_eq_getElem (show 63 + (i - 52 - 11) < 64 by omega)]
      congr 1 <;> omega

theorem unpack_inf_exp_man (b : BitVec 64) (s : Sign)
    (h : unpack Format.binary64 b = .infinity s) :
    unpackExponent (spec := Format.binary64) b = 2047#11 ∧
      unpackMantissa (spec := Format.binary64) b = 0#52 := by
  by_cases he : unpackExponent (spec := Format.binary64) b = 2047#11
  · simp [UnpackedFloat.unpack, he] at h
    by_cases hm : unpackMantissa (spec := Format.binary64) b = 0#52
    · exact ⟨he, hm⟩
    · simp [UnpackedFloat.unpack, he, hm] at h
  · by_cases he0 : unpackExponent (spec := Format.binary64) b = 0#11
    · simp [UnpackedFloat.unpack, he, he0] at h
      split at h <;> simp at h
    · simp [UnpackedFloat.unpack, he, he0] at h

theorem unpack_inf_sign (b : BitVec 64) (s : Sign)
    (h : unpack Format.binary64 b = .infinity s) :
    unpackSign (spec := Format.binary64) b = s.toBitVec := by
  by_cases he : unpackExponent (spec := Format.binary64) b = 2047#11
  · simp [UnpackedFloat.unpack, he] at h
    by_cases hm : unpackMantissa (spec := Format.binary64) b = 0#52
    · have hs : Sign.ofBitVec (unpackSign (spec := Format.binary64) b) = s := by
        simpa [UnpackedFloat.unpack, he, hm] using h
      rw [← signBit_ofBitVec (unpackSign (spec := Format.binary64) b)]
      exact congrArg Sign.toBitVec hs
    · simp [UnpackedFloat.unpack, he, hm] at h
  · by_cases he0 : unpackExponent (spec := Format.binary64) b = 0#11
    · simp [UnpackedFloat.unpack, he, he0] at h
      split at h <;> simp at h
    · simp [UnpackedFloat.unpack, he, he0] at h

theorem unpack_zero_exp_man (b : BitVec 64) (s : Sign)
    (h : unpack Format.binary64 b = .zero s) :
    unpackExponent (spec := Format.binary64) b = 0#11 ∧
      unpackMantissa (spec := Format.binary64) b = 0#52 := by
  by_cases he : unpackExponent (spec := Format.binary64) b = 2047#11
  · simp [UnpackedFloat.unpack, he] at h
    split at h <;> simp at h
  · by_cases he0 : unpackExponent (spec := Format.binary64) b = 0#11
    · simp [UnpackedFloat.unpack, he, he0] at h
      by_cases hm : unpackMantissa (spec := Format.binary64) b = 0#52
      · exact ⟨he0, hm⟩
      · simp [UnpackedFloat.unpack, he, he0, hm] at h
    · simp [UnpackedFloat.unpack, he, he0] at h

theorem unpack_zero_sign (b : BitVec 64) (s : Sign)
    (h : unpack Format.binary64 b = .zero s) :
    unpackSign (spec := Format.binary64) b = s.toBitVec := by
  by_cases he : unpackExponent (spec := Format.binary64) b = 2047#11
  · simp [UnpackedFloat.unpack, he] at h
    split at h <;> simp at h
  · by_cases he0 : unpackExponent (spec := Format.binary64) b = 0#11
    · simp [UnpackedFloat.unpack, he, he0] at h
      by_cases hm : unpackMantissa (spec := Format.binary64) b = 0#52
      · have hs : Sign.ofBitVec (unpackSign (spec := Format.binary64) b) = s := by
          simpa [UnpackedFloat.unpack, he, he0, hm] using h
        rw [← signBit_ofBitVec (unpackSign (spec := Format.binary64) b)]
        exact congrArg Sign.toBitVec hs
      · simp [UnpackedFloat.unpack, he, he0, hm] at h
    · simp [UnpackedFloat.unpack, he, he0] at h

theorem unpack_finite_normal (b : BitVec 64) (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (h : unpack Format.binary64 b = .finite s m e hm)
    (he : unpackExponent (spec := Format.binary64) b ≠ 2047#11)
    (he0 : unpackExponent (spec := Format.binary64) b ≠ 0#11) :
    m = (1#1 ++ unpackMantissa (spec := Format.binary64) b).toNat ∧
      e = (unpackExponent (spec := Format.binary64) b).toNat - (Format.binary64.exponentBias + 52) ∧
      unpackSign (spec := Format.binary64) b = s.toBitVec := by
  simp [UnpackedFloat.unpack, he, he0] at h
  constructor
  · simpa [BitVec.toNat_append] using h.2.1.symm
  · constructor
    · exact h.2.2.symm
    · rw [← signBit_ofBitVec (unpackSign (spec := Format.binary64) b)]
      exact congrArg Sign.toBitVec h.1

theorem unpack_finite_sub (b : BitVec 64) (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (h : unpack Format.binary64 b = .finite s m e hm)
    (he : unpackExponent (spec := Format.binary64) b ≠ 2047#11)
    (he0 : unpackExponent (spec := Format.binary64) b = 0#11) :
    m = (unpackMantissa (spec := Format.binary64) b).toNat ∧
      e = (0 : Int) - (Format.binary64.exponentBias + 52) + 1 ∧
      unpackSign (spec := Format.binary64) b = s.toBitVec := by
  simp [UnpackedFloat.unpack, he, he0] at h
  split at h
  · next hzero => simp [hzero] at h
  · next hnz =>
      simp at h
      constructor
      · exact h.2.1.symm
      · constructor
        · exact h.2.2.symm
        · rw [← signBit_ofBitVec (unpackSign (spec := Format.binary64) b)]
          exact congrArg Sign.toBitVec h.1

theorem compare_finite_eq (s1 s2 : Sign) (m1 m2 : Nat) (e1 e2 : Int) (h1 : 0 < m1) (h2 : 0 < m2)
    (hcmp : UnpackedFloat.compare (UnpackedFloat.finite s1 m1 e1 h1)
      (UnpackedFloat.finite s2 m2 e2 h2) = some Ordering.eq) :
    s1 = s2 ∧ e1 = e2 ∧ m1 = m2 := by
  cases s1 <;> cases s2 <;> simp [Float.Model.UnpackedFloat.compare] at hcmp ⊢
  · exact hcmp
  · exact hcmp

theorem unpackSign_64 (b : BitVec 64) :
    unpackSign (spec := Format.binary64) b = b.extractLsb' 63 1 := by
  simp [unpackSign]

theorem unpackExponent_64 (b : BitVec 64) :
    unpackExponent (spec := Format.binary64) b = b.extractLsb' 52 11 := by
  simp [unpackExponent]

theorem unpackMantissa_64 (b : BitVec 64) :
    unpackMantissa (spec := Format.binary64) b = b.extractLsb' 0 52 := by
  simp [unpackMantissa]

theorem bits_eq_of_unpack_inf (b c : BitVec 64) (s1 s2 : Sign)
    (hb : unpack Format.binary64 b = .infinity s1)
    (hc : unpack Format.binary64 c = .infinity s2) (hs : s1 = s2) : b = c := by
  have hbE := unpack_inf_exp_man b s1 hb
  have hcE := unpack_inf_exp_man c s2 hc
  have hbS := unpack_inf_sign b s1 hb
  have hcS := unpack_inf_sign c s2 hc
  subst s2
  rw [bitvec64_decompose b, bitvec64_decompose c]
  rw [← unpackSign_64 b, ← unpackSign_64 c, hbS, hcS]
  rw [← unpackExponent_64 b, ← unpackExponent_64 c, hbE.1, hcE.1]
  rw [← unpackMantissa_64 b, ← unpackMantissa_64 c, hbE.2, hcE.2]

theorem bits_eq_of_unpack_finite (b c : BitVec 64) (s1 s2 : Sign) (m1 m2 : Nat) (e1 e2 : Int)
    (hm1 : 0 < m1) (hm2 : 0 < m2)
    (hb : unpack Format.binary64 b = .finite s1 m1 e1 hm1)
    (hc : unpack Format.binary64 c = .finite s2 m2 e2 hm2)
    (hcmp : UnpackedFloat.compare (.finite s1 m1 e1 hm1) (.finite s2 m2 e2 hm2) = some Ordering.eq) :
    b = c := by
  have heq := compare_finite_eq s1 s2 m1 m2 e1 e2 hm1 hm2 hcmp
  rcases heq with ⟨hs, he, hm⟩
  have hbE : unpackExponent (spec := Format.binary64) b ≠ 2047#11 := by
    intro h
    simp [UnpackedFloat.unpack, h] at hb
    split at hb <;> simp at hb
  have hcE : unpackExponent (spec := Format.binary64) c ≠ 2047#11 := by
    intro h
    simp [UnpackedFloat.unpack, h] at hc
    split at hc <;> simp at hc
  by_cases hbE0 : unpackExponent (spec := Format.binary64) b = 0#11
  · have hb' := unpack_finite_sub b s1 m1 e1 hm1 hb hbE hbE0
    by_cases hcE0 : unpackExponent (spec := Format.binary64) c = 0#11
    · have hc' := unpack_finite_sub c s2 m2 e2 hm2 hc hcE hcE0
      have hmanB : (unpackMantissa (spec := Format.binary64) b).toNat =
          (unpackMantissa (spec := Format.binary64) c).toNat := by omega
      have hman : unpackMantissa (spec := Format.binary64) b =
          unpackMantissa (spec := Format.binary64) c := BitVec.eq_of_toNat_eq hmanB
      have hsB := hb'.2.2
      have hsC := hc'.2.2
      rw [bitvec64_decompose b, bitvec64_decompose c]
      rw [← unpackSign_64 b, ← unpackSign_64 c, hsB, hsC, hs]
      rw [← unpackExponent_64 b, ← unpackExponent_64 c, hbE0, hcE0]
      rw [← unpackMantissa_64 b, ← unpackMantissa_64 c, hman]
    · have hc' := unpack_finite_normal c s2 m2 e2 hm2 hc hcE hcE0
      have hcManLt : (unpackMantissa (spec := Format.binary64) c).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (by omega : 52 ≤ 52)
      have hbManLt : (unpackMantissa (spec := Format.binary64) b).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (by omega : 52 ≤ 52)
      have hge : 2 ^ 52 ≤ (1#1 ++ unpackMantissa (spec := Format.binary64) c).toNat := by
        simp [BitVec.toNat_append]
        exact Nat.left_le_or
      omega
  · have hb' := unpack_finite_normal b s1 m1 e1 hm1 hb hbE hbE0
    by_cases hcE0 : unpackExponent (spec := Format.binary64) c = 0#11
    · have hc' := unpack_finite_sub c s2 m2 e2 hm2 hc hcE hcE0
      have hcManLt : (unpackMantissa (spec := Format.binary64) c).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (by omega : 52 ≤ 52)
      have hbManLt : (unpackMantissa (spec := Format.binary64) b).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (by omega : 52 ≤ 52)
      have hge : 2 ^ 52 ≤ (1#1 ++ unpackMantissa (spec := Format.binary64) b).toNat := by
        simp [BitVec.toNat_append]
        exact Nat.left_le_or
      omega
    · have hc' := unpack_finite_normal c s2 m2 e2 hm2 hc hcE hcE0
      have hfull : (1#1 ++ unpackMantissa (spec := Format.binary64) b) =
          (1#1 ++ unpackMantissa (spec := Format.binary64) c) := by
        apply BitVec.eq_of_toNat_eq
        omega
      have hbManLt : (unpackMantissa (spec := Format.binary64) b).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (by omega : 52 ≤ 52)
      have hcManLt : (unpackMantissa (spec := Format.binary64) c).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (by omega : 52 ≤ 52)
      have hto := congrArg BitVec.toNat hfull
      simp [BitVec.toNat_append] at hto
      rw [← Nat.two_pow_add_eq_or_of_lt hbManLt 1,
          ← Nat.two_pow_add_eq_or_of_lt hcManLt 1] at hto
      have hmanNat : (unpackMantissa (spec := Format.binary64) b).toNat =
          (unpackMantissa (spec := Format.binary64) c).toNat := by omega
      have hman : unpackMantissa (spec := Format.binary64) b =
          unpackMantissa (spec := Format.binary64) c := BitVec.eq_of_toNat_eq hmanNat
      have hexpNatB : (unpackExponent (spec := Format.binary64) b).toNat =
          (unpackExponent (spec := Format.binary64) c).toNat := by omega
      have hexp : unpackExponent (spec := Format.binary64) b =
          unpackExponent (spec := Format.binary64) c := BitVec.eq_of_toNat_eq hexpNatB
      have hsB := hb'.2.2
      have hsC := hc'.2.2
      rw [bitvec64_decompose b, bitvec64_decompose c]
      rw [← unpackSign_64 b, ← unpackSign_64 c, hsB, hsC, hs]
      rw [← unpackExponent_64 b, ← unpackExponent_64 c, hexp]
      rw [← unpackMantissa_64 b, ← unpackMantissa_64 c, hman]

theorem ieee_compare (a b : Float) :
    (a == b) = (Float.Model.UnpackedFloat.compare a.toModel.unpack b.toModel.unpack == some Ordering.eq) := by
  change (Float.Model.beq a.toModel b.toModel) = _
  simp [Float.Model.beq, Float.Model.UnpackedFloat.beq]

theorem eq_zero_of_unpack_zero (a : Float) (s : Sign) (h : a.toModel.unpack = .zero s) :
    (a == 0) = true := by
  rw [ieee_compare]
  have h0 : (0 : Float).toModel.unpack = .zero Sign.positive := rfl
  simp [h, h0, UnpackedFloat.compare]

theorem bits_or_zero_of_ieee_eq (a b : Float) (h : (a == b) = true) :
    a.toBits = b.toBits ∨ ((a == 0) = true ∧ (b == 0) = true) := by
  have hcmp : Float.Model.UnpackedFloat.compare a.toModel.unpack b.toModel.unpack = some Ordering.eq := by
    have hc := ieee_compare a b
    simpa [h] using hc.symm
  cases ha : a.toModel.unpack with
  | notANumber =>
      rw [ha] at hcmp
      simp [Float.Model.UnpackedFloat.compare] at hcmp
  | infinity s1 =>
      cases hb : b.toModel.unpack with
      | notANumber =>
          rw [ha, hb] at hcmp
          simp [Float.Model.UnpackedFloat.compare] at hcmp
      | infinity s2 =>
          rw [ha, hb] at hcmp
          have hs : s1 = s2 := by
            cases s1 <;> cases s2 <;> simp [Float.Model.UnpackedFloat.compare] at hcmp ⊢
          left
          have haU : unpack Format.binary64 a.toModel.toBits.toBitVec = .infinity s1 := by
            simpa [Float.Model.unpack] using ha
          have hbU : unpack Format.binary64 b.toModel.toBits.toBitVec = .infinity s2 := by
            simpa [Float.Model.unpack] using hb
          have hbvec := bits_eq_of_unpack_inf a.toModel.toBits.toBitVec b.toModel.toBits.toBitVec s1 s2 haU hbU hs
          exact congrArg UInt64.ofBitVec hbvec
      | zero s2 =>
          rw [ha, hb] at hcmp
          cases s1 <;> cases s2 <;> simp [Float.Model.UnpackedFloat.compare] at hcmp
      | finite s2 m2 e2 hm2 =>
          rw [ha, hb] at hcmp
          cases s1 <;> cases s2 <;> simp [Float.Model.UnpackedFloat.compare] at hcmp
  | zero s1 =>
      cases hb : b.toModel.unpack with
      | notANumber =>
          rw [ha, hb] at hcmp
          simp [Float.Model.UnpackedFloat.compare] at hcmp
      | infinity s2 =>
          rw [ha, hb] at hcmp
          cases s1 <;> cases s2 <;> simp [Float.Model.UnpackedFloat.compare] at hcmp
      | zero s2 =>
          right
          exact ⟨eq_zero_of_unpack_zero a s1 ha, eq_zero_of_unpack_zero b s2 hb⟩
      | finite s2 m2 e2 hm2 =>
          rw [ha, hb] at hcmp
          cases s1 <;> cases s2 <;> simp [Float.Model.UnpackedFloat.compare] at hcmp
  | finite s1 m1 e1 hm1 =>
      cases hb : b.toModel.unpack with
      | notANumber =>
          rw [ha, hb] at hcmp
          simp [Float.Model.UnpackedFloat.compare] at hcmp
      | infinity s2 =>
          rw [ha, hb] at hcmp
          cases s1 <;> cases s2 <;> simp [Float.Model.UnpackedFloat.compare] at hcmp
      | zero s2 =>
          rw [ha, hb] at hcmp
          cases s1 <;> cases s2 <;> simp [Float.Model.UnpackedFloat.compare] at hcmp
      | finite s2 m2 e2 hm2 =>
          left
          have haU : unpack Format.binary64 a.toModel.toBits.toBitVec = .finite s1 m1 e1 hm1 := by
            simpa [Float.Model.unpack] using ha
          have hbU : unpack Format.binary64 b.toModel.toBits.toBitVec = .finite s2 m2 e2 hm2 := by
            simpa [Float.Model.unpack] using hb
          have hcmp' : UnpackedFloat.compare (UnpackedFloat.finite s1 m1 e1 hm1)
              (UnpackedFloat.finite s2 m2 e2 hm2) = some Ordering.eq := by
            simpa [ha, hb] using hcmp
          have hbvec := bits_eq_of_unpack_finite a.toModel.toBits.toBitVec b.toModel.toBits.toBitVec
            s1 s2 m1 m2 e1 e2 hm1 hm2 haU hbU hcmp'
          exact congrArg UInt64.ofBitVec hbvec

theorem model_eq_of_toBits_eq (a b : Float) (h : a.toBits = b.toBits) : a.toModel = b.toModel := by
  cases a with
  | ofModel am =>
  cases b with
  | ofModel bm =>
    cases am with
    | mk ab av =>
    cases bm with
    | mk bb bv =>
      change ab = bb at h
      subst bb
      rfl

theorem ieee_eq_of_bits_eq (a b : Float) (hbits : a.toBits = b.toBits) (hna : a.isNaN = false) :
    (a == b) = true := by
  rw [ieee_compare]
  have hm := model_eq_of_toBits_eq a b hbits
  cases ha : a.toModel.unpack with
  | notANumber =>
      simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, ha] at hna
  | infinity s =>
      have hbu : b.toModel.unpack = .infinity s := by rw [← hm]; exact ha
      cases s <;> simp [hbu, UnpackedFloat.compare] <;> native_decide
  | zero s =>
      have hbu : b.toModel.unpack = .zero s := by rw [← hm]; exact ha
      simp [hbu, UnpackedFloat.compare]
  | finite s m e hm2 =>
      have hbu : b.toModel.unpack = .finite s m e hm2 := by rw [← hm]; exact ha
      cases s <;> simp [hbu, UnpackedFloat.compare] <;> native_decide

theorem ieee_eq_of_both_zero (a b : Float) (ha : (a == 0) = true) (hb : (b == 0) = true) :
    (a == b) = true := by
  have h0 : (0 : Float).toModel.unpack = .zero Sign.positive := rfl
  rw [ieee_compare]
  rw [ieee_compare] at ha hb
  cases hau : a.toModel.unpack with
  | notANumber =>
      rw [hau] at ha
      simp [h0, UnpackedFloat.compare] at ha
  | infinity s =>
      cases s <;> rw [hau] at ha <;> simp [h0, UnpackedFloat.compare] at ha
  | finite s m e hm =>
      cases s <;> rw [hau] at ha <;> simp [h0, UnpackedFloat.compare] at ha
  | zero sa =>
      cases hbu : b.toModel.unpack with
      | notANumber =>
          rw [hbu] at hb
          simp [h0, UnpackedFloat.compare] at hb
      | infinity s =>
          cases s <;> rw [hbu] at hb <;> simp [h0, UnpackedFloat.compare] at hb
      | finite s m e hm =>
          cases s <;> rw [hbu] at hb <;> simp [h0, UnpackedFloat.compare] at hb
      | zero sb => simp [hau, hbu, UnpackedFloat.compare]

theorem not_nan_of_ieee_eq_left (a b : Float) (h : (a == b) = true) : a.isNaN = false := by
  rw [ieee_compare] at h
  cases ha : a.toModel.unpack with
  | notANumber => rw [ha] at h; simp [UnpackedFloat.compare] at h
  | infinity s => simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, ha]
  | zero s => simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, ha]
  | finite s m e hm => simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, ha]

theorem not_nan_of_ieee_eq_right (a b : Float) (h : (a == b) = true) : b.isNaN = false := by
  rw [ieee_compare] at h
  cases hb : b.toModel.unpack with
  | notANumber => rw [hb] at h; cases hau : a.toModel.unpack <;> rw [hau] at h <;> simp [UnpackedFloat.compare] at h
  | infinity s => simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, hb]
  | zero s => simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, hb]
  | finite s m e hm => simp [Float.isNaN, Float.Model.isNaN, UnpackedFloat.isNaN, hb]

theorem eq_zero_of_bits_eq (a b : Float) (ha : (a == 0) = true) (hbits : a.toBits = b.toBits) :
    (b == 0) = true := by
  rw [ieee_compare] at ha
  rw [ieee_compare]
  have hm := model_eq_of_toBits_eq a b hbits
  have h0 : (0 : Float).toModel.unpack = .zero Sign.positive := rfl
  cases hau : a.toModel.unpack with
  | notANumber => rw [hau] at ha; simp [h0, UnpackedFloat.compare] at ha
  | infinity s => cases s <;> rw [hau] at ha <;> simp [h0, UnpackedFloat.compare] at ha
  | finite s m e hm2 => cases s <;> rw [hau] at ha <;> simp [h0, UnpackedFloat.compare] at ha
  | zero s =>
      have hbu : b.toModel.unpack = .zero s := by rw [← hm]; exact hau
      simp [hbu, h0, UnpackedFloat.compare]

theorem isNaN_of_key_none (a : Float) (h : key a = none) : a.isNaN = true := by
  by_cases hn : a.isNaN = true
  · exact hn
  · simp [key, hn] at h

theorem not_nan_of_key_some (a : Float) (k : UInt64) (h : key a = some k) : a.isNaN = false := by
  cases hn : a.isNaN <;> simp [key, hn] at h ⊢

theorem canonical_key_eq (a b : Float) :
    (a == b) = ((key a).any (fun k => key b == some k)) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have hna := not_nan_of_ieee_eq_left a b h
    have hnb := not_nan_of_ieee_eq_right a b h
    rcases bits_or_zero_of_ieee_eq a b h with hbits | ⟨ha0, hb0⟩
    · by_cases haZ : (a == 0) = true
      · have hbZ : (b == 0) = true := eq_zero_of_bits_eq a b haZ hbits
        simp [key, hna, hnb, haZ, hbZ]
      · have hbZ : (b == 0) = false := by
          by_cases hbz : (b == 0) = true
          · have hz : (a == 0) = true := eq_zero_of_bits_eq b a hbz hbits.symm
            exact (haZ hz).elim
          · cases hbz' : (b == 0) <;> simp [hbz'] at hbz ⊢
        simp [key, hna, hnb, haZ, hbZ, hbits]
    · simp [key, hna, hnb, ha0, hb0]
  · intro h
    cases hka : key a with
    | none =>
        have hna : a.isNaN = true := isNaN_of_key_none a hka
        simp [hka] at h
    | some ka =>
        have hna : a.isNaN = false := not_nan_of_key_some a ka hka
        cases hkb : key b with
        | none => simp [hkb] at h
        | some kb =>
            have hnb : b.isNaN = false := not_nan_of_key_some b kb hkb
            have hkeq : ka = kb := (by simpa [hka, hkb] using h : kb = ka).symm
            by_cases hk0 : ka = 0
            · have ha0 : (a == 0) = true := by
                by_cases haZ : (a == 0) = true
                · exact haZ
                · have hka' : a.toBits = ka := by simpa [key, hna, haZ] using hka
                  have hbits0 : a.toBits = (0 : Float).toBits := by change a.toBits = 0; exact hka'.trans hk0
                  exact ieee_eq_of_bits_eq a 0 hbits0 hna
              have hb0 : (b == 0) = true := by
                by_cases hbZ : (b == 0) = true
                · exact hbZ
                · have hkb' : b.toBits = kb := by simpa [key, hnb, hbZ] using hkb
                  have hbits0 : b.toBits = (0 : Float).toBits := by change b.toBits = 0; exact hkb'.trans (hkeq.symm.trans hk0)
                  exact ieee_eq_of_bits_eq b 0 hbits0 hnb
              exact ieee_eq_of_both_zero a b ha0 hb0
            · have haZ : (a == 0) = false := by
                by_cases ha0 : (a == 0) = true
                · have hka0 : ka = 0 := by
                    simpa [key, hna, ha0] using hka.symm
                  exact (hk0 hka0).elim
                · cases ha0' : (a == 0) <;> simp [ha0'] at ha0 ⊢
              have hbZ : (b == 0) = false := by
                by_cases hb0 : (b == 0) = true
                · have hkb0 : kb = 0 := by
                    simpa [key, hnb, hb0] using hkb.symm
                  exact (hk0 (hkeq.trans hkb0)).elim
                · cases hb0' : (b == 0) <;> simp [hb0'] at hb0 ⊢
              have hka' : a.toBits = ka := by simpa [key, hna, haZ] using hka
              have hkb' : b.toBits = kb := by simpa [key, hnb, hbZ] using hkb
              exact ieee_eq_of_bits_eq a b (hka'.trans (hkeq.trans hkb'.symm)) hna

theorem or3_true (a b : Truth) : or3 a b = some true ↔ a = some true ∨ b = some true := by
  cases a with
  | none =>
      cases b with
      | none => simp [or3]
      | some bv => cases bv <;> simp [or3]
  | some av =>
      cases av with
      | false =>
          cases b with
          | none => simp [or3]
          | some bv => cases bv <;> simp [or3]
      | true =>
          cases b with
          | none => simp [or3]
          | some bv => cases bv <;> simp [or3]

theorem foldl_or3_true (x : Cell) (l : List Cell) (acc : Truth) :
    (l.foldl (fun a y => or3 a (equal x y)) acc = some true) ↔
      acc = some true ∨ ∃ y ∈ l, equal x y = some true := by
  induction l generalizing acc with
  | nil => simp
  | cons y ys ih =>
      rw [List.foldl_cons]
      rw [ih]
      rw [or3_true]
      simp [or_assoc, or_left_comm, or_comm]

theorem rescan_true_iff (x : Cell) (l : List Cell) :
    rescan x l = some true ↔ ∃ y ∈ l, equal x y = some true := by
  rw [rescan, foldl_or3_true]
  simp

theorem equal_false_iff_not_true (x y : Cell) :
    (equal x y = some false) ↔ ¬ equal x y = some true ∧ ¬ equal x y = none := by
  cases x <;> cases y <;> simp [equal]

theorem foldl_or3_false (x : Cell) (l : List Cell) (acc : Truth) :
    (l.foldl (fun a y => or3 a (equal x y)) acc = some false) ↔
      acc = some false ∧ ∀ y ∈ l, equal x y = some false := by
  induction l generalizing acc with
  | nil => simp
  | cons y ys ih =>
      rw [List.foldl_cons]
      rw [ih]
      cases ha : acc with
      | none =>
          cases hb : equal x y with
          | none => simp [or3, ha, hb] at ih ⊢
          | some bv => cases bv <;> simp [or3, ha, hb] at ih ⊢
      | some av =>
          cases av with
          | false =>
              cases hb : equal x y with
              | none => simp [or3, ha, hb] at ih ⊢
              | some bv => cases bv <;> simp [or3, ha, hb] at ih ⊢
          | true =>
              cases hb : equal x y with
              | none => simp [or3, ha, hb] at ih ⊢
              | some bv => cases bv <;> simp [or3, ha, hb] at ih ⊢

theorem rescan_false_iff (x : Cell) (l : List Cell) :
    rescan x l = some false ↔ ∀ y ∈ l, equal x y = some false := by
  rw [rescan, foldl_or3_false]
  simp

theorem equal_true_iff_key (x y : Cell) :
    (equal x y = some true) ↔ ((x.bind key).any (fun k => (y.bind key) == some k)) = true := by
  cases x with
  | none => simp [equal]
  | some xv =>
      cases y with
      | none => simp [equal]
      | some yv =>
          simp [equal, canonical_key_eq]

theorem equal_false_and_not_null (x y : Cell) (hx : x.isNone = false) (hy : y.isNone = false)
    (hno : ¬ ((x.bind key).any (fun k => (y.bind key) == some k)) = true) :
    equal x y = some false := by
  cases x with
  | none => simp at hx
  | some xv =>
      cases y with
      | none => simp at hy
      | some yv =>
          simp [equal]
          have htrue : (xv == yv) = false := by
            have hc := canonical_key_eq xv yv
            have hk : ((key xv).any (fun k => key yv == some k)) = false := by
              by_cases h' : ((key xv).any (fun k => key yv == some k)) = true
              · exact (hno h').elim
              · cases h'' : ((key xv).any (fun k => key yv == some k)) <;> simp [h''] at h' ⊢
            simpa [hk] using hc
          simpa [htrue]

theorem keys_contains_iff (xs : List Cell) (k : UInt64) :
    ((build xs).keys.contains k) = true ↔ ∃ y ∈ xs, (y.bind key) = some k := by
  simp [build, List.mem_eraseDups, List.mem_filterMap]

theorem foldl_or3_none (l : List Cell) : l.foldl (fun acc z => or3 acc none) none = none := by
  induction l with
  | nil => rfl
  | cons z zs ih =>
      change List.foldl (fun acc z => or3 acc none) (or3 none none) zs = none
      rw [show or3 none none = none by rfl, ih]

theorem rescan_none_of_nonempty (xs : List Cell) (h : xs ≠ []) : rescan none xs = none := by
  cases xs with
  | nil => simp at h
  | cons y ys =>
      change List.foldl (fun acc z => or3 acc none) none ys = none
      exact foldl_or3_none ys

theorem rescan_none_of_null_no_true (x : Cell) (xs : List Cell)
    (hnull : ∃ y ∈ xs, y.isNone = true) (hnottrue : ∀ y ∈ xs, equal x y ≠ some true) :
    rescan x xs = none := by
  have hntrue : rescan x xs ≠ some true := by
    intro hr
    rcases (rescan_true_iff x xs).mp hr with ⟨y, hy, heq⟩
    exact hnottrue y hy heq
  have hnfalse : rescan x xs ≠ some false := by
    intro hr
    have hall := (rescan_false_iff x xs).mp hr
    rcases hnull with ⟨y, hy, hyis⟩
    have hf := hall y hy
    cases y with
    | none => simp [equal] at hf
    | some yv => simp at hyis
  cases hr : rescan x xs with
  | none => rfl
  | some b => cases b <;> simp [hr] at hntrue hnfalse

theorem probe_build_of_hit (xs : List Cell) (x : Cell) (kk : UInt64) (hcnt : xs.length ≠ 0)
    (hxkey : x.bind key = some kk) (hhit : (build xs).keys.contains kk = true) :
    probe (build xs) x = some true := by
  have hc : (build xs).count ≠ 0 := by simp [build, hcnt]
  have hxne : x ≠ none := by cases x <;> simp at hxkey ⊢
  simp [probe, hc, hxkey, hxne]
  simpa using hhit

theorem probe_build_of_nohit_nonull (xs : List Cell) (x : Cell) (kk : UInt64) (hcnt : xs.length ≠ 0)
    (hxkey : x.bind key = some kk) (hnohit : (build xs).keys.contains kk = false)
    (hnonull : xs.any Option.isNone = false) :
    probe (build xs) x = some false := by
  have hc : (build xs).count ≠ 0 := by simp [build, hcnt]
  have hxne : x ≠ none := by cases x <;> simp at hxkey ⊢
  have hnullB : (build xs).hasNull = false := by simp [build, hnonull]
  simp [probe, hc, hxkey, hxne, hnullB]
  simpa using hnohit

theorem probe_build_of_nohit_null (xs : List Cell) (x : Cell) (kk : UInt64) (hcnt : xs.length ≠ 0)
    (hxkey : x.bind key = some kk) (hnohit : (build xs).keys.contains kk = false)
    (hnull : xs.any Option.isNone = true) :
    probe (build xs) x = none := by
  have hc : (build xs).count ≠ 0 := by simp [build, hcnt]
  have hxne : x ≠ none := by cases x <;> simp at hxkey ⊢
  have hnullB : (build xs).hasNull = true := by simp [build, hnull]
  simp [probe, hc, hxkey, hxne, hnullB]
  simpa using hnohit

theorem probe_build_nan (xs : List Cell) (xv : Float) (hcnt : xs.length ≠ 0)
    (hxkey : (some xv).bind key = none) (hnull : xs.any Option.isNone = false) :
    probe (build xs) (some xv) = some false := by
  have hc : (build xs).count ≠ 0 := by simp [build, hcnt]
  have hnullB : (build xs).hasNull = false := by simp [build, hnull]
  simp [probe, hc, hxkey, hnullB]

theorem probe_build_nan_null (xs : List Cell) (xv : Float) (hcnt : xs.length ≠ 0)
    (hxkey : (some xv).bind key = none) (hnull : xs.any Option.isNone = true) :
    probe (build xs) (some xv) = none := by
  have hc : (build xs).count ≠ 0 := by simp [build, hcnt]
  have hnullB : (build xs).hasNull = true := by simp [build, hnull]
  simp [probe, hc, hxkey, hnullB]

theorem floating_in_refines_rescan : FloatingInSubqueryRefinesRescan.Goal := by
  intro xs x
  cases x with
  | none =>
      by_cases hempty : xs = []
      · simp [hempty, rescan, build, probe]
      · simp [probe, build, hempty, rescan_none_of_nonempty xs hempty]
  | some xv =>
      by_cases hempty : xs = []
      · simp [hempty, rescan, build, probe]
      · have hcnt : xs.length ≠ 0 := by
          cases xs with
          | nil => simp at hempty
          | cons y ys => simp
        by_cases hnan : xv.isNaN = true
        · have hxkey : (some xv).bind key = none := by simp [key, hnan]
          have hnottrue : ∀ y ∈ xs, equal (some xv) y ≠ some true := by
            intro y hy heq
            cases y with
            | none => simp [equal] at heq
            | some yv =>
                have hv : (xv == yv) = true := by simpa [equal] using heq
                have hn := not_nan_of_ieee_eq_left xv yv hv
                exact (by simpa [hnan] using hn)
          by_cases hnull : xs.any Option.isNone = true
          · rcases (List.any_eq_true.mp hnull) with ⟨y, hy, hyis⟩
            have hresnone := rescan_none_of_null_no_true (some xv) xs ⟨y, hy, hyis⟩ hnottrue
            simp [probe, build, hcnt, hxkey, hnull, hresnone]
          · have hallfalse : ∀ y ∈ xs, equal (some xv) y = some false := by
              intro y hy
              cases y with
              | none => exact (hnull (List.any_eq_true.mpr ⟨none, hy, rfl⟩)).elim
              | some yv =>
                  have hv : (xv == yv) = false := by
                    by_cases h : (xv == yv) = true
                    · exact (by simpa [hnan] using (not_nan_of_ieee_eq_left xv yv h))
                    · cases h' : (xv == yv) <;> simp [h'] at h ⊢
                  simp [equal, hv]
            have hresfalse : rescan (some xv) xs = some false := (rescan_false_iff _ _).mpr hallfalse
            simp [probe, build, hcnt, hxkey, hnull, hresfalse]
        · let kk : UInt64 := ((key xv).getD 0)
          have hxkey : (some xv).bind key = some kk := by simp [key, hnan, kk]
          have hxkey0 : key xv = some kk := by simpa using hxkey
          by_cases hhit : (build xs).keys.contains kk = true
          · have hex : ∃ y ∈ xs, y.bind key = some kk := (keys_contains_iff xs kk).mp hhit
            have hrestrue : rescan (some xv) xs = some true := by
              apply (rescan_true_iff (some xv) xs).mpr
              rcases hex with ⟨y, hy, hykey⟩
              cases y with
              | none => simp at hykey
              | some yv =>
                  refine ⟨some yv, hy, ?_⟩
                  simp [equal]
                  have hykv : key yv = some kk := by simpa using hykey
                  have hkeq : key yv = key xv := hykv.trans hxkey0.symm
                  have hc := canonical_key_eq xv yv
                  rw [hc, hxkey0, hkeq]
                  simp [hxkey0]
            rw [probe_build_of_hit xs (some xv) kk hcnt hxkey hhit, hrestrue]
          · have hnottrue : ∀ y ∈ xs, equal (some xv) y ≠ some true := by
              intro y hy heq
              cases y with
              | none => simp [equal] at heq
              | some yv =>
                  have heq' : ((key xv).any (fun k => key yv == some k)) = true := by
                    have hc := canonical_key_eq xv yv
                    rw [← hc]
                    simpa [equal] using heq
                  have hkey : (some yv).bind key = some kk := by simpa [hxkey0] using heq'
                  exact hhit ((keys_contains_iff xs kk).mpr ⟨some yv, hy, hkey⟩)
            by_cases hnull : xs.any Option.isNone = true
            · rcases (List.any_eq_true.mp hnull) with ⟨y, hy, hyis⟩
              have hresnone := rescan_none_of_null_no_true (some xv) xs ⟨y, hy, hyis⟩ hnottrue
              have hnohitB : (build xs).keys.contains kk = false := by
                cases h : (build xs).keys.contains kk
                · rfl
                · exact (hhit h).elim
              rw [probe_build_of_nohit_null xs (some xv) kk hcnt hxkey hnohitB hnull, hresnone]
            · have hresfalse : rescan (some xv) xs = some false := (rescan_false_iff _ _).mpr (by
                intro y hy
                cases y with
                | none => exact (hnull (List.any_eq_true.mpr ⟨none, hy, rfl⟩)).elim
                | some yv =>
                    have hvfalse : (xv == yv) = false := by
                      by_cases ht : (xv == yv) = true
                      · have heq' : ((key xv).any (fun k => key yv == some k)) = true := by
                          have hc := canonical_key_eq xv yv
                          rw [← hc]
                          exact ht
                        have hkeyv : key yv = some kk := by simpa [hxkey0] using heq'
                        have hc : (build xs).keys.contains kk = true :=
                          (keys_contains_iff xs kk).mpr ⟨some yv, hy, by simpa [hxkey0] using hkeyv⟩
                        exact (hhit hc).elim
                      · cases hvv : (xv == yv) <;> simp [hvv] at ht ⊢
                    simp [equal, hvfalse])
              have hnohitB : (build xs).keys.contains kk = false := by
                cases h : (build xs).keys.contains kk
                · rfl
                · exact (hhit h).elim
              have hnonull : xs.any Option.isNone = false := by
                cases h : xs.any Option.isNone
                · rfl
                · exact (hnull h).elim
              rw [probe_build_of_nohit_nonull xs (some xv) kk hcnt hxkey hnohitB hnonull, hresfalse]

theorem sound : FloatingNotInFilterRefinesRescan.Goal := by
  intro outerRows innerRows
  apply List.filter_congr
  intro x hx
  rw [floating_in_refines_rescan]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_NullableMembership_FloatingNotInFilterRefinesRescan
