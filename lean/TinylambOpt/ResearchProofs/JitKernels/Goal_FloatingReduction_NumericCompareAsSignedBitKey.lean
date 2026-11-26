import Std
import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
import TinylambOpt.ResearchProofs.JitKernels.Common_FloatingReduction_Bits

/-! For finite strictly positive binary64 values the sign bit is zero, and the
   exponent and mantissa fields occupy fixed bit ranges. IEEE comparison is
   lexicographic in (exponent, mantissa), which is exactly unsigned integer
   order of the whole 64-bit pattern. A sorted scan may therefore compare the
   raw bits of positive keys without decoding them again. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_NumericCompareAsSignedBitKey

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionBits
open Float.Model
open Float.Model.UnpackedFloat

private theorem unpack_pos_finite (x : Float) (hfin : TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.finite x = true) (hpos : 0 < x) :
    ∃ m e hm, x.toModel.unpack = .finite .positive m e hm := by
  simp only [TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.finite, Float.isFinite, Float.Model.isFinite, UnpackedFloat.isFinite] at hfin
  change Float.Model.lt (0 : Float).toModel x.toModel = true at hpos
  unfold Float.Model.lt at hpos
  cases hx : x.toModel.unpack with
  | infinity s => simp [hx] at hfin
  | notANumber => simp [hx] at hfin
  | zero s =>
      have h0 : (0 : Float).toModel.unpack = .zero .positive := by rfl
      rw [h0, hx] at hpos
      simp [UnpackedFloat.lt, UnpackedFloat.compare] at hpos
  | finite s m e hm =>
      cases s
      · have h0 : (0 : Float).toModel.unpack = .zero .positive := by rfl
        rw [h0, hx] at hpos
        simp [UnpackedFloat.lt, UnpackedFloat.compare] at hpos
      · exact ⟨m, e, hm, rfl⟩

theorem sound : NumericCompareAsSignedBitKey.Goal := by
  intro a b hfa hfb hpa hpb
  obtain ⟨ma, ea, hma, ha⟩ := unpack_pos_finite a hfa hpa
  obtain ⟨mb, eb, hmb, hb⟩ := unpack_pos_finite b hfb hpb
  change (Float.Model.lt a.toModel b.toModel = true) ↔ (bits a).toNat < (bits b).toNat
  unfold Float.Model.lt
  rw [ha, hb, lt_finite_pos_iff_lex]
  unfold bits Float.toBits
  change _ ↔ a.toModel.toBits.toBitVec.toNat < b.toModel.toBits.toBitVec.toNat
  rw [split_bv a.toModel.toBits.toBitVec, split_bv b.toModel.toBits.toBitVec]
  obtain ⟨hsa, hfa'⟩ := unpack_finite_fields a.toModel.toBits.toBitVec Sign.positive ma ea hma ha
  obtain ⟨hsb, hfb'⟩ := unpack_finite_fields b.toModel.toBits.toBitVec Sign.positive mb eb hmb hb
  have hsa0 : unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec = 0#1 := by simpa [Sign.toBitVec] using hsa
  have hsb0 : unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec = 0#1 := by simpa [Sign.toBitVec] using hsb
  rw [hsa0, hsb0]
  rw [← BitVec.lt_def]
  rcases hfa' with ⟨hea, hma0, hmaeq, heaeq⟩ | ⟨hea, heaall, hmaeq, heaeq⟩
  · rcases hfb' with ⟨heb, hmb0, hmbeq, hebeq⟩ | ⟨heb, _, hmbeq, hebeq⟩
    · -- both subnormal
      rw [append_lt_iff]
      simp [hea, heb, heaeq, hebeq, hmaeq, hmbeq, BitVec.lt_def]
    · -- a subnormal, b normal
      have hlt_exp : (0#11 : BitVec 11) < unpackExponent (spec := Format.binary64) b.toModel.toBits.toBitVec :=
        zero_exp_lt_of_ne_zero _ heb
      have hlex : ea < eb ∨ (ea = eb ∧ ma < mb) := by
        by_cases h1 : (unpackExponent (spec := Format.binary64) b.toModel.toBits.toBitVec).toNat = 1
        · right
          constructor
          · rw [heaeq, hebeq]
            simp [Format.exponentBias, h1]
          · have hma_lt : ma < 4503599627370496 := by
              rw [hmaeq]
              simpa using (unpackMantissa (spec:=Format.binary64) a.toModel.toBits.toBitVec).isLt
            have hmb_ge : 4503599627370496 ≤ mb := by
              rw [hmbeq]
              exact Nat.left_le_or
            omega
        · left
          have hne0 : (unpackExponent (spec:=Format.binary64) b.toModel.toBits.toBitVec).toNat ≠ 0 := by
            intro hz
            apply heb
            apply BitVec.toNat_inj.mp
            simpa using hz
          rw [heaeq, hebeq]
          simp only [Format.exponentBias]
          omega
      rw [append_lt_iff]
      simp [hea, hlt_exp, hlex]
  · rcases hfb' with ⟨heb, _, hmbeq, hebeq⟩ | ⟨heb, _, hmbeq, hebeq⟩
    · -- a normal, b subnormal
      have hnlt_exp : ¬ unpackExponent (spec := Format.binary64) a.toModel.toBits.toBitVec < (0#11 : BitVec 11) :=
        not_exp_lt_zero_of_ne_zero _ hea
      have hnlex : ¬ (ea < eb ∨ (ea = eb ∧ ma < mb)) := by
        rw [heaeq, hebeq]
        simp only [Format.exponentBias]
        intro h
        rcases h with hlt | ⟨heq, hmlt⟩
        · have hne0 : (unpackExponent (spec:=Format.binary64) a.toModel.toBits.toBitVec).toNat ≠ 0 := by
            intro hz
            apply hea
            apply BitVec.toNat_inj.mp
            simpa using hz
          omega
        · have hma_ge : 2 ^ 52 ≤ ma := by rw [hmaeq]; exact Nat.left_le_or
          have hmb_lt : mb < 2 ^ 52 := by rw [hmbeq]; exact (unpackMantissa (spec:=Format.binary64) b.toModel.toBits.toBitVec).isLt
          omega
      rw [append_lt_iff]
      simp [hea, heb, hnlt_exp, hnlex]
    · -- both normal
      have hexp : (unpackExponent (spec := Format.binary64) a.toModel.toBits.toBitVec <
          unpackExponent (spec := Format.binary64) b.toModel.toBits.toBitVec) ↔ ea < eb := by
        rw [BitVec.lt_def, heaeq, hebeq]
        simp [Format.exponentBias]
      have hexpeq : (unpackExponent (spec := Format.binary64) a.toModel.toBits.toBitVec =
          unpackExponent (spec := Format.binary64) b.toModel.toBits.toBitVec) ↔ ea = eb := by
        rw [heaeq, hebeq]
        simp only [Format.exponentBias]
        constructor
        · intro h
          have ht := congrArg (fun b : BitVec 11 => (b.toNat : Int)) h
          omega
        · intro h
          apply BitVec.toNat_inj.mp
          omega
      have hmant : (unpackMantissa (spec := Format.binary64) a.toModel.toBits.toBitVec <
          unpackMantissa (spec := Format.binary64) b.toModel.toBits.toBitVec) ↔ ma < mb := by
        rw [hmaeq, hmbeq]
        change _ ↔ ((1#1 ++ unpackMantissa (spec := Format.binary64) a.toModel.toBits.toBitVec : BitVec 53) <
          (1#1 ++ unpackMantissa (spec := Format.binary64) b.toModel.toBits.toBitVec : BitVec 53))
        rw [append_one_lt_iff]
      rw [append_lt_iff]
      constructor
      · intro h
        rcases h with hlt | ⟨heq, hmlt⟩
        · exact Or.inl (hexp.mpr hlt)
        · exact Or.inr ⟨hexpeq.mpr heq, hmant.mpr hmlt⟩
      · intro h
        rcases h with hlt | ⟨heq, hflt⟩
        · exact Or.inl (hexp.mp hlt)
        · exact Or.inr ⟨hexpeq.mp heq, hmant.mp hflt⟩

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_NumericCompareAsSignedBitKey
