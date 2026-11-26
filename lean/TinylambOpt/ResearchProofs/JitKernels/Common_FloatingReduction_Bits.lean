import Std
import TinylambOpt.ResearchProofs.JitKernels.Common_FloatingReduction

/-! Bit-level inversion facts for the IEEE-754 `Float.Model` decoder.

A valid `Float.Model` is a 64-bit pattern whose `NaN` encodings are canonical.
The lemmas here recover that pattern from the decoded `UnpackedFloat`, which
lets kernel proofs identify the observable bits of two floats that decode to
the same value. These facts support canonicalization and comparison rewrites
in floating-point reductions without assuming anything extra about NaN payloads.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReductionBits

open Float.Model
open Float.Model.UnpackedFloat
open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionCommon

/- Reassembling the three decoded fields recovers the original pattern. -/

theorem split_bv (bv : BitVec 64) :
    bv = unpackSign (spec := Format.binary64) bv ++ unpackExponent (spec := Format.binary64) bv ++
      unpackMantissa (spec := Format.binary64) bv := by
  simpa [unpackSign, unpackExponent, unpackMantissa] using
    (show bv = bv.extractLsb' 63 1 ++ bv.extractLsb' 52 11 ++ bv.extractLsb' 0 52 from by bv_decide)

private theorem sign_eq_bitvec {b : BitVec 1} : (Sign.ofBitVec b).toBitVec = b := by
  cases b <;> simp [Sign.ofBitVec, Sign.toBitVec] <;> bv_decide

/-- Decoding to a special value pins down the corresponding fields. -/

theorem unpack_inf_fields (bv : BitVec 64) (s : Sign)
    (h : UnpackedFloat.unpack Format.binary64 bv = .infinity s) :
    unpackExponent (spec := Format.binary64) bv = BitVec.allOnes 11 ∧
      unpackMantissa (spec := Format.binary64) bv = 0#52 ∧
      unpackSign (spec := Format.binary64) bv = s.toBitVec := by
  have hallones : (2047#11 : BitVec 11) = BitVec.allOnes 11 := by decide
  unfold UnpackedFloat.unpack at h
  simp only [BitVec.neg_one_eq_allOnes] at h
  by_cases he : unpackExponent (spec := Format.binary64) bv = BitVec.allOnes 11
  · simp only [he, ↓reduceIte] at h
    by_cases hm : unpackMantissa (spec := Format.binary64) bv = 0#52
    · simp only [hm, ↓reduceIte] at h
      simp at h
      have hs' : unpackSign (spec := Format.binary64) bv = s.toBitVec := by
        simpa [sign_eq_bitvec] using congrArg Sign.toBitVec h
      exact ⟨he, hm, hs'⟩
    · simp only [hm, ↓reduceIte] at h
      cases h
  · simp only [he, ↓reduceIte] at h
    split at h
    · split at h <;> simp_all
    · simp_all

theorem unpack_zero_fields (bv : BitVec 64) (s : Sign)
    (h : UnpackedFloat.unpack Format.binary64 bv = .zero s) :
    unpackExponent (spec := Format.binary64) bv = 0#11 ∧
      unpackMantissa (spec := Format.binary64) bv = 0#52 ∧
      unpackSign (spec := Format.binary64) bv = s.toBitVec := by
  have hallones : (2047#11 : BitVec 11) = BitVec.allOnes 11 := by decide
  unfold UnpackedFloat.unpack at h
  simp only [BitVec.neg_one_eq_allOnes] at h
  by_cases he1 : unpackExponent (spec := Format.binary64) bv = BitVec.allOnes 11
  · simp only [he1, ↓reduceIte] at h
    split at h <;> cases h
  · simp only [he1, ↓reduceIte] at h
    by_cases he0 : unpackExponent (spec := Format.binary64) bv = 0#11
    · simp only [he0, ↓reduceIte] at h
      by_cases hm : unpackMantissa (spec := Format.binary64) bv = 0#52
      · simp only [hm, ↓reduceIte] at h
        simp at h
        have hs' : unpackSign (spec := Format.binary64) bv = s.toBitVec := by
          simpa [sign_eq_bitvec] using congrArg Sign.toBitVec h
        exact ⟨he0, hm, hs'⟩
      · simp only [hm, ↓reduceIte] at h
        cases h
    · simp only [he0, ↓reduceIte] at h
      simp_all

theorem unpack_nan_fields (bv : BitVec 64)
    (h : UnpackedFloat.unpack Format.binary64 bv = .notANumber) :
    unpackExponent (spec := Format.binary64) bv = BitVec.allOnes 11 ∧
      unpackMantissa (spec := Format.binary64) bv ≠ 0#52 := by
  have hallones : (2047#11 : BitVec 11) = BitVec.allOnes 11 := by decide
  unfold UnpackedFloat.unpack at h
  simp only [BitVec.neg_one_eq_allOnes] at h
  by_cases he1 : unpackExponent (spec := Format.binary64) bv = BitVec.allOnes 11
  · simp only [he1, ↓reduceIte] at h
    by_cases hm : unpackMantissa (spec := Format.binary64) bv = 0#52
    · simp only [hm, ↓reduceIte] at h
      cases h
    · simp only [hm, ↓reduceIte] at h
      exact ⟨he1, hm⟩
  · simp only [he1, ↓reduceIte] at h
    split at h
    · split at h <;> cases h
    · cases h

/- Decoding to a finite value either gives a subnormal (zero exponent field)
   or a normal (nonzero, non-all-ones exponent field) mantissa/exponent pair. -/

theorem unpack_finite_fields (bv : BitVec 64) (s : Sign) (m : Nat) (e : Int) (hm : 0 < m)
    (h : UnpackedFloat.unpack Format.binary64 bv = .finite s m e hm) :
    unpackSign (spec := Format.binary64) bv = s.toBitVec ∧
      ((unpackExponent (spec := Format.binary64) bv = 0#11 ∧
          unpackMantissa (spec := Format.binary64) bv ≠ 0#52 ∧
          m = (unpackMantissa (spec := Format.binary64) bv).toNat ∧ e = -1074) ∨
       (unpackExponent (spec := Format.binary64) bv ≠ 0#11 ∧
          unpackExponent (spec := Format.binary64) bv ≠ BitVec.allOnes 11 ∧
          m = (1#1 ++ unpackMantissa (spec := Format.binary64) bv).toNat ∧
          e = (unpackExponent (spec := Format.binary64) bv).toNat - 1075)) := by
  have hallones : (2047#11 : BitVec 11) = BitVec.allOnes 11 := by decide
  unfold UnpackedFloat.unpack at h
  simp only [BitVec.neg_one_eq_allOnes] at h
  by_cases he1 : unpackExponent (spec := Format.binary64) bv = BitVec.allOnes 11
  · simp only [he1, ↓reduceIte] at h
    split at h <;> cases h
  · simp only [he1, ↓reduceIte] at h
    by_cases he0 : unpackExponent (spec := Format.binary64) bv = 0#11
    · simp only [he0, ↓reduceIte] at h
      by_cases hm0 : unpackMantissa (spec := Format.binary64) bv = 0#52
      · simp only [hm0, ↓reduceIte] at h
        cases h
      · simp only [hm0, ↓reduceIte] at h
        injection h with hs hm' he'
        have hs' : unpackSign (spec := Format.binary64) bv = s.toBitVec := by
          simpa [sign_eq_bitvec] using congrArg Sign.toBitVec hs
        simp [Format.exponentBias] at he'
        exact ⟨hs', Or.inl ⟨he0, hm0, hm'.symm, by omega⟩⟩
    · simp only [he0, ↓reduceIte] at h
      injection h with hs hm' he'
      have hs' : unpackSign (spec := Format.binary64) bv = s.toBitVec := by
        simpa [sign_eq_bitvec] using congrArg Sign.toBitVec hs
      simp [Format.exponentBias] at he'
      exact ⟨hs', Or.inr ⟨he0, he1, hm'.symm, by omega⟩⟩

/- A valid pattern that decodes to a special value is exactly the canonical
   packed encoding of that value. -/

theorem unpack_inf_eq_pack (bv : BitVec 64) (s : Sign)
    (h : UnpackedFloat.unpack Format.binary64 bv = .infinity s) :
    bv = packedInfinity Format.binary64 s := by
  obtain ⟨he, hm, hs⟩ := unpack_inf_fields bv s h
  rw [split_bv bv, he, hm, hs]
  simp [packedInfinity, packComponents]

theorem unpack_zero_eq_pack (bv : BitVec 64) (s : Sign)
    (h : UnpackedFloat.unpack Format.binary64 bv = .zero s) :
    bv = packedZero Format.binary64 s := by
  obtain ⟨he, hm, hs⟩ := unpack_zero_fields bv s h
  rw [split_bv bv, he, hm, hs]
  simp [packedZero, packComponents]

/- Decoding is injective on valid patterns: IEEE-equal non-NaN values that are
   not both zero therefore have identical observable bits. -/

theorem unpack_inj {a b : Float.Model} (h : a.unpack = b.unpack) : a.toBits = b.toBits := by
  apply congrArg UInt64.ofBitVec
  let ba := a.toBits.toBitVec
  let bb := b.toBits.toBitVec
  change ba = bb
  cases ha : a.unpack with
  | notANumber =>
      have hb : b.unpack = .notANumber := by simpa [ha] using h.symm
      obtain ⟨hea, hma⟩ := unpack_nan_fields ba ha
      obtain ⟨heb, hmb⟩ := unpack_nan_fields bb hb
      have hnana := a.valid.eq_packedNaN (by simpa [Float.Model.unpack, ba] using hea) (by simpa [Float.Model.unpack, ba] using hma)
      have hnanb := b.valid.eq_packedNaN (by simpa [Float.Model.unpack, bb] using heb) (by simpa [Float.Model.unpack, bb] using hmb)
      simpa [ba, bb, Float.Model.unpack] using hnana.trans hnanb.symm
  | infinity s =>
      have hb : b.unpack = .infinity s := by simpa [ha] using h.symm
      rw [unpack_inf_eq_pack ba s ha, unpack_inf_eq_pack bb s hb]
  | zero s =>
      have hb : b.unpack = .zero s := by simpa [ha] using h.symm
      rw [unpack_zero_eq_pack ba s ha, unpack_zero_eq_pack bb s hb]
  | finite s m e hm =>
      have hb : b.unpack = .finite s m e hm := by simpa [ha] using h.symm
      obtain ⟨hsa, hfa⟩ := unpack_finite_fields ba s m e hm ha
      obtain ⟨hsb, hfb⟩ := unpack_finite_fields bb s m e hm hb
      rw [split_bv ba, split_bv bb, hsa, hsb]
      rcases hfa with ⟨hea, hma, hmaeq, heaeq⟩ | ⟨hea, heaall, hmaeq, heaeq⟩
      · rcases hfb with ⟨heb, hmb, hmbeq, hebeq⟩ | ⟨heb, _, hmbeq, _⟩
        · have hmant : unpackMantissa (spec:=Format.binary64) ba = unpackMantissa (spec:=Format.binary64) bb := by
            apply BitVec.toNat_inj.mp
            omega
          simp [hea, heb, hmant]
        · have hlt : m < 2 ^ 52 := by rw [hmaeq]; exact (unpackMantissa (spec:=Format.binary64) ba).isLt
          have hge : 2 ^ 52 ≤ m := by rw [hmbeq]; exact Nat.left_le_or
          omega
      · rcases hfb with ⟨heb, _, hmbeq, _⟩ | ⟨heb, _, hmbeq, hebeq⟩
        · have hlt : m < 2 ^ 52 := by rw [hmbeq]; exact (unpackMantissa (spec:=Format.binary64) bb).isLt
          have hge : 2 ^ 52 ≤ m := by rw [hmaeq]; exact Nat.left_le_or
          omega
        · have ht : ((unpackExponent (spec:=Format.binary64) ba).toNat : Int) - 1075 =
              ((unpackExponent (spec:=Format.binary64) bb).toNat : Int) - 1075 := heaeq.symm.trans hebeq
          have hexp : unpackExponent (spec:=Format.binary64) ba = unpackExponent (spec:=Format.binary64) bb := by
            apply BitVec.toNat_inj.mp
            omega
          have hmant : unpackMantissa (spec:=Format.binary64) ba = unpackMantissa (spec:=Format.binary64) bb := by
            have happ : (1#1 ++ unpackMantissa (spec:=Format.binary64) ba).toNat =
                (1#1 ++ unpackMantissa (spec:=Format.binary64) bb).toNat := by omega
            exact (BitVec.append_right_inj (1#1)).mp (BitVec.toNat_inj.mp happ)
          simp [hexp, hmant]


/- IEEE comparison of two positive finite unpacked values is lexicographic in
   (exponent, mantissa). -/

theorem lt_finite_pos_iff_lex (ma mb : Nat) (ea eb : Int) (hma : 0 < ma) (hmb : 0 < mb) :
    (UnpackedFloat.lt (.finite .positive ma ea hma) (.finite .positive mb eb hmb) = true) ↔
      (ea < eb ∨ (ea = eb ∧ ma < mb)) := by
  simp [UnpackedFloat.lt, UnpackedFloat.compare, Ordering.then_eq_lt, Int.compare_eq_lt, Nat.compare_eq_lt]

/- With a common zero sign bit, unsigned order of the packed pattern is exactly
   lexicographic order of the exponent and mantissa fields. -/

theorem append_lt_iff (ea eb : BitVec 11) (fa fb : BitVec 52) :
    ((0#1 ++ ea ++ fa : BitVec 64) < (0#1 ++ eb ++ fb : BitVec 64)) ↔
      (ea < eb ∨ (ea = eb ∧ fa < fb)) := by
  bv_decide

theorem zero_exp_lt_of_ne_zero (e : BitVec 11) (h : e ≠ 0#11) : (0#11 : BitVec 11) < e := by
  bv_decide

theorem not_exp_lt_zero_of_ne_zero (e : BitVec 11) (h : e ≠ 0#11) : ¬ e < (0#11 : BitVec 11) := by
  bv_decide


theorem append_one_lt_iff (fa fb : BitVec 52) :
    ((1#1 ++ fa : BitVec 53) < (1#1 ++ fb : BitVec 53)) ↔ fa < fb := by
  bv_decide

end TinylambOpt.ResearchProofs.JitKernels.FloatingReductionBits
