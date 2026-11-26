import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_NumericCompareAsSignedBitKey

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open Float.Model
open Float.Model.UnpackedFloat

abbrev B := BitVec 64
abbrev E := BitVec 11
abbrev F := BitVec 52

def ex (b : B) : E := unpackExponent (spec := Float.Model.Format.binary64) b
def fr (b : B) : F := unpackMantissa (spec := Float.Model.Format.binary64) b
def sg (b : B) : BitVec 1 := unpackSign (spec := Float.Model.Format.binary64) b
def zeroUnpack : Float.Model.UnpackedFloat := unpack Float.Model.Format.binary64 (0#64)
def lex (b1 b2 : B) : Prop :=
  (ex b1).toNat < (ex b2).toNat ∨
    ((ex b1).toNat = (ex b2).toNat ∧ (fr b1).toNat < (fr b2).toNat)

/-- For finite positive binary64 values, numeric order is the lexicographic
order of the encoded exponent and significand fields. The proof unfolds the
IEEE-754 decoder, eliminates signed zero / infinities / NaNs using the hypotheses,
and handles subnormals separately from normals. -/
theorem sound : TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.NumericCompareAsSignedBitKey.Goal := by
  have hzeroBits : (0 : Float).toBits = 0 := by native_decide
  have hzeroModelBits : (0 : Float).toModel.toBits = 0 := by native_decide
  have bit1_of_ne_zero : ∀ x : BitVec 1, x ≠ 0#1 → x = 1#1 := by
    intro x hx
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    have hi0 : i = 0 := by omega
    subst i
    have hx0 : x.getLsbD 0 = true := by
      cases hbit : x.getLsbD 0 <;> simp_all
      exfalso
      apply hx
      apply BitVec.eq_of_getLsbD_eq
      intro j hj
      have hj0 : j = 0 := by omega
      subst j
      simp [hbit]
    simp [hx0]
  have derive : ∀ b : B,
      (unpack Float.Model.Format.binary64 b).isFinite = true →
      zeroUnpack.lt (unpack Float.Model.Format.binary64 b) = true →
      sg b = 0#1 ∧ ex b ≠ 2047#11 ∧ ¬ (ex b = 0#11 ∧ fr b = 0#52) := by
    intro b hfin hpos
    have hzex : unpackExponent (spec := Float.Model.Format.binary64) (0#64) = 0#11 := by native_decide
    have hzfr : unpackMantissa (spec := Float.Model.Format.binary64) (0#64) = 0#52 := by native_decide
    have hzsg : unpackSign (spec := Float.Model.Format.binary64) (0#64) = 0#1 := by native_decide
    constructor
    · by_cases hsg0 : unpackSign (spec := Float.Model.Format.binary64) b = 0#1
      · exact hsg0
      · have hsgn : unpackSign (spec := Float.Model.Format.binary64) b = 1#1 :=
          bit1_of_ne_zero _ hsg0
        by_cases hmax0 : unpackExponent (spec := Float.Model.Format.binary64) b = 2047#11
        · by_cases hfr0 : unpackMantissa (spec := Float.Model.Format.binary64) b = 0#52
          · simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
                  Float.Model.UnpackedFloat.unpack, zeroUnpack, hzex, hzfr, hzsg,
                  hmax0, hfr0, hsgn, Sign.ofBitVec] at hpos
          · simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
                  Float.Model.UnpackedFloat.unpack, zeroUnpack, hzex, hzfr, hzsg,
                  hmax0, hfr0, hsgn, Sign.ofBitVec] at hpos
        · by_cases hzeroexp0 : unpackExponent (spec := Float.Model.Format.binary64) b = 0#11
          · by_cases hfr00 : unpackMantissa (spec := Float.Model.Format.binary64) b = 0#52
            · simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
                    Float.Model.UnpackedFloat.unpack, zeroUnpack, hzex, hzfr, hzsg,
                    hmax0, hzeroexp0, hfr00, hsgn, Sign.ofBitVec] at hpos
            · simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
                    Float.Model.UnpackedFloat.unpack, zeroUnpack, hzex, hzfr, hzsg,
                    hmax0, hzeroexp0, hfr00, hsgn, Sign.ofBitVec] at hpos
          · simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
                  Float.Model.UnpackedFloat.unpack, zeroUnpack, hzex, hzfr, hzsg,
                  hmax0, hzeroexp0, hsgn, Sign.ofBitVec] at hpos
    constructor
    · by_cases hmax0 : unpackExponent (spec := Float.Model.Format.binary64) b = 2047#11
      · by_cases hfr0 : unpackMantissa (spec := Float.Model.Format.binary64) b = 0#52
        · simp [Float.Model.UnpackedFloat.isFinite, Float.Model.UnpackedFloat.unpack,
                hmax0, hfr0] at hfin
        · simp [Float.Model.UnpackedFloat.isFinite, Float.Model.UnpackedFloat.unpack,
                hmax0, hfr0] at hfin
      · exact hmax0
    · by_cases hzeroexp0 : unpackExponent (spec := Float.Model.Format.binary64) b = 0#11
      · by_cases hfr00 : unpackMantissa (spec := Float.Model.Format.binary64) b = 0#52
        · simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
                Float.Model.UnpackedFloat.unpack, zeroUnpack, hzex, hzfr, hzsg,
                hzeroexp0, hfr00, Sign.ofBitVec] at hpos
        · intro h
          exact hfr00 h.2
      · intro h
        exact hzeroexp0 h.1
  have lex_iff : ∀ b1 b2 : B,
      sg b1 = 0#1 → sg b2 = 0#1 →
      ex b1 ≠ 2047#11 → ex b2 ≠ 2047#11 →
      (ex b1 = 0#11 → (fr b1).toNat ≠ 0) →
      (ex b2 = 0#11 → (fr b2).toNat ≠ 0) →
      ((unpack Float.Model.Format.binary64 b1).lt
          (unpack Float.Model.Format.binary64 b2) = true ↔ lex b1 b2) := by
    intro b1
    intro b2
    intro hs1
    intro hs2
    intro hm1
    intro hm2
    intro hz1
    intro hz2
    have hf1 : (fr b1).toNat < 2^52 := by
      unfold fr
      rw [Float.Model.UnpackedFloat.unpackMantissa, BitVec.extractLsb'_toNat]
      simp [Nat.shiftRight_eq_div_pow]
      exact Nat.mod_lt _ (by native_decide : 0 < 2^52)
    have hf2 : (fr b2).toNat < 2^52 := by
      unfold fr
      rw [Float.Model.UnpackedFloat.unpackMantissa, BitVec.extractLsb'_toNat]
      simp [Nat.shiftRight_eq_div_pow]
      exact Nat.mod_lt _ (by native_decide : 0 < 2^52)
    have hor1 : (2^52 : Nat) ||| (fr b1).toNat = 2^52 + (fr b1).toNat := by
      rw [Nat.or_comm, Nat.or_two_pow_eq_add_of_lt hf1, Nat.add_comm]
    have hor2 : (2^52 : Nat) ||| (fr b2).toNat = 2^52 + (fr b2).toNat := by
      rw [Nat.or_comm, Nat.or_two_pow_eq_add_of_lt hf2, Nat.add_comm]
    unfold lex ex fr sg at *
    by_cases h01 : unpackExponent (spec := Float.Model.Format.binary64) b1 = 0#11
    · have hfrn1 := hz1 h01
      have hfr01 : unpackMantissa (spec := Float.Model.Format.binary64) b1 ≠ 0#52 := by
        intro h
        apply hfrn1
        rw [h]
        rfl
      by_cases h02 : unpackExponent (spec := Float.Model.Format.binary64) b2 = 0#11
      · have hfrn2 := hz2 h02
        have hfr02 : unpackMantissa (spec := Float.Model.Format.binary64) b2 ≠ 0#52 := by
          intro h
          apply hfrn2
          rw [h]
          rfl
        simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
              Float.Model.UnpackedFloat.unpack, Sign.ofBitVec, hs1, hs2, hm1, hm2,
              h01, h02, hfr01, hfr02, hor1, hor2]
        by_cases hf : (unpackMantissa (spec := Float.Model.Format.binary64) b1).toNat <
            (unpackMantissa (spec := Float.Model.Format.binary64) b2).toNat
        · have hltBV : unpackMantissa (spec := Float.Model.Format.binary64) b1 <
              unpackMantissa (spec := Float.Model.Format.binary64) b2 := by
            simpa [BitVec.lt_def] using hf
          simp [compare, compareOfLessAndEq, hltBV, hf]
        · have hnltBV : ¬ (unpackMantissa (spec := Float.Model.Format.binary64) b1 <
              unpackMantissa (spec := Float.Model.Format.binary64) b2) := by
            simpa [BitVec.lt_def] using hf
          by_cases heqf : unpackMantissa (spec := Float.Model.Format.binary64) b1 =
              unpackMantissa (spec := Float.Model.Format.binary64) b2 <;>
            simp [compare, compareOfLessAndEq, hnltBV, hf, heqf]
      · have he2nz : (unpackExponent (spec := Float.Model.Format.binary64) b2).toNat ≠ 0 := by
          intro h0
          apply h02
          apply BitVec.eq_of_toNat_eq
          simp [h0]
        simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
              Float.Model.UnpackedFloat.unpack, Sign.ofBitVec, hs1, hs2, hm1, hm2,
              h01, h02, hfr01, hor1, hor2]
        by_cases he1 : (unpackExponent (spec := Float.Model.Format.binary64) b2).toNat = 1
        · have hflt : (unpackMantissa (spec := Float.Model.Format.binary64) b1).toNat <
              (4503599627370496 : Nat) +
                (unpackMantissa (spec := Float.Model.Format.binary64) b2).toNat := by omega
          have heqI : (-((Format.binary64.exponentBias : Int) + 52) + 1) =
              (1 - ((Format.binary64.exponentBias : Int) + 52)) := by omega
          simp [compare, compareOfLessAndEq, he1, hflt, heqI, he2nz]
        · have hltI : (-((Format.binary64.exponentBias : Int) + 52) + 1 : Int) <
              ((unpackExponent (spec := Float.Model.Format.binary64) b2).toNat : Int) -
                ((Format.binary64.exponentBias : Int) + 52) := by omega
          have hpos : 0 < (unpackExponent (spec := Float.Model.Format.binary64) b2).toNat := by omega
          simp [compare, compareOfLessAndEq, hltI, he1, hpos]
    · by_cases h02 : unpackExponent (spec := Float.Model.Format.binary64) b2 = 0#11
      · have hfrn2 := hz2 h02
        have hfr02 : unpackMantissa (spec := Float.Model.Format.binary64) b2 ≠ 0#52 := by
          intro h
          apply hfrn2
          rw [h]
          rfl
        have he1nz : (unpackExponent (spec := Float.Model.Format.binary64) b1).toNat ≠ 0 := by
          intro h0
          apply h01
          apply BitVec.eq_of_toNat_eq
          simp [h0]
        simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
              Float.Model.UnpackedFloat.unpack, Sign.ofBitVec, hs1, hs2, hm1, hm2,
              h01, h02, hfr02, hor1, hor2]
        by_cases he1 : (unpackExponent (spec := Float.Model.Format.binary64) b1).toNat = 1
        · have hflt : (unpackMantissa (spec := Float.Model.Format.binary64) b2).toNat <
              (4503599627370496 : Nat) +
                (unpackMantissa (spec := Float.Model.Format.binary64) b1).toNat := by omega
          have heqI : (1 - ((Format.binary64.exponentBias : Int) + 52)) =
              (-((Format.binary64.exponentBias : Int) + 52) + 1) := by omega
          have hngt : ¬ ((4503599627370496 : Nat) +
                (unpackMantissa (spec := Float.Model.Format.binary64) b1).toNat <
              (unpackMantissa (spec := Float.Model.Format.binary64) b2).toNat) := by omega
          have hne : (4503599627370496 : Nat) +
                (unpackMantissa (spec := Float.Model.Format.binary64) b1).toNat ≠
              (unpackMantissa (spec := Float.Model.Format.binary64) b2).toNat := by omega
          simp [compare, compareOfLessAndEq, he1, heqI, hngt, hne, he1nz]
        · have hltI : ((unpackExponent (spec := Float.Model.Format.binary64) b1).toNat : Int) -
                ((Format.binary64.exponentBias : Int) + 52) >
              (-((Format.binary64.exponentBias : Int) + 52) + 1 : Int) := by omega
          have hnltI : ¬ (((unpackExponent (spec := Float.Model.Format.binary64) b1).toNat : Int) -
                ((Format.binary64.exponentBias : Int) + 52) <
              (-((Format.binary64.exponentBias : Int) + 52) + 1 : Int)) := by omega
          have hneI : ((unpackExponent (spec := Float.Model.Format.binary64) b1).toNat : Int) -
                ((Format.binary64.exponentBias : Int) + 52) ≠
              (-((Format.binary64.exponentBias : Int) + 52) + 1 : Int) := by omega
          simp [compare, compareOfLessAndEq, hltI, hnltI, hneI, he1, he1nz]
      · have he1nz : (unpackExponent (spec := Float.Model.Format.binary64) b1).toNat ≠ 0 := by
          intro h0
          apply h01
          apply BitVec.eq_of_toNat_eq
          simp [h0]
        have he2nz : (unpackExponent (spec := Float.Model.Format.binary64) b2).toNat ≠ 0 := by
          intro h0
          apply h02
          apply BitVec.eq_of_toNat_eq
          simp [h0]
        simp [Float.Model.UnpackedFloat.lt, Float.Model.UnpackedFloat.compare,
              Float.Model.UnpackedFloat.unpack, Sign.ofBitVec, hs1, hs2, hm1, hm2,
              h01, h02, hor1, hor2]
        by_cases he : (unpackExponent (spec := Float.Model.Format.binary64) b1).toNat <
            (unpackExponent (spec := Float.Model.Format.binary64) b2).toNat
        · have hlt : ((unpackExponent (spec := Float.Model.Format.binary64) b1).toNat : Int) -
                ((Format.binary64.exponentBias : Int) + 52) <
              ((unpackExponent (spec := Float.Model.Format.binary64) b2).toNat : Int) -
                ((Format.binary64.exponentBias : Int) + 52) := by omega
          simp [compare, compareOfLessAndEq, hlt, he]
        · by_cases heq : (unpackExponent (spec := Float.Model.Format.binary64) b1).toNat =
              (unpackExponent (spec := Float.Model.Format.binary64) b2).toNat
          · by_cases hf : (unpackMantissa (spec := Float.Model.Format.binary64) b1).toNat <
                (unpackMantissa (spec := Float.Model.Format.binary64) b2).toNat
            · have hfI : (4503599627370496 : Nat) +
                    (unpackMantissa (spec := Float.Model.Format.binary64) b1).toNat <
                  (4503599627370496 : Nat) +
                    (unpackMantissa (spec := Float.Model.Format.binary64) b2).toNat := by omega
              simp [compare, compareOfLessAndEq, hf, he, heq, hfI]
            · have hnltI : ¬ ((4503599627370496 : Nat) +
                    (unpackMantissa (spec := Float.Model.Format.binary64) b1).toNat <
                  (4503599627370496 : Nat) +
                    (unpackMantissa (spec := Float.Model.Format.binary64) b2).toNat) := by omega
              by_cases heqf : (unpackMantissa (spec := Float.Model.Format.binary64) b1).toNat =
                  (unpackMantissa (spec := Float.Model.Format.binary64) b2).toNat <;>
                simp [compare, compareOfLessAndEq, hf, he, heq, hnltI, heqf]
          · have hgt : (unpackExponent (spec := Float.Model.Format.binary64) b2).toNat <
                (unpackExponent (spec := Float.Model.Format.binary64) b1).toNat := by omega
            have hltI : ((unpackExponent (spec := Float.Model.Format.binary64) b2).toNat : Int) -
                  ((Format.binary64.exponentBias : Int) + 52) <
                ((unpackExponent (spec := Float.Model.Format.binary64) b1).toNat : Int) -
                  ((Format.binary64.exponentBias : Int) + 52) := by omega
            have hnltI : ¬ (((unpackExponent (spec := Float.Model.Format.binary64) b1).toNat : Int) -
                  ((Format.binary64.exponentBias : Int) + 52) <
                ((unpackExponent (spec := Float.Model.Format.binary64) b2).toNat : Int) -
                  ((Format.binary64.exponentBias : Int) + 52)) := by omega
            have hneI : ((unpackExponent (spec := Float.Model.Format.binary64) b1).toNat : Int) -
                  ((Format.binary64.exponentBias : Int) + 52) ≠
                ((unpackExponent (spec := Float.Model.Format.binary64) b2).toNat : Int) -
                  ((Format.binary64.exponentBias : Int) + 52) := by omega
            simp [compare, compareOfLessAndEq, hnltI, hneI, he, heq]
  have decomp : ∀ b : B,
      sg b = 0#1 → b.toNat = (ex b).toNat * 2^52 + (fr b).toNat := by
    intro b hs
    have hs' : unpackSign (spec := Float.Model.Format.binary64) b = 0#1 := hs
    have hsign0 : (unpackSign (spec := Float.Model.Format.binary64) b).toNat = 0 := by
      rw [hs']
      rfl
    rw [unpackSign, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow] at hsign0
    change b.toNat / 2^63 % 2 = 0 at hsign0
    have hq : b.toNat / 2^63 < 2 := by
      have hb := b.isLt
      rw [Nat.div_lt_iff_lt_mul (by native_decide : 0 < 2^63)]
      omega
    have hq0 : b.toNat / 2^63 = 0 := by
      cases hcase : b.toNat / 2^63 with
      | zero => rfl
      | succ q' =>
          have hq1 : q' + 1 = 1 := by omega
          rw [hcase] at hsign0
          simp [hq1] at hsign0
    have hb63 : b.toNat < 2^63 := Nat.lt_of_div_eq_zero (by native_decide : 0 < 2^63) hq0
    have hex : (ex b).toNat = b.toNat / 2^52 := by
      unfold ex
      rw [unpackExponent, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
      have hq52 : b.toNat / 2^52 < 2^11 := by
        rw [Nat.div_lt_iff_lt_mul (by native_decide : 0 < 2^52)]
        omega
      rw [Nat.mod_eq_of_lt hq52]
    have hfr : (fr b).toNat = b.toNat % 2^52 := by
      unfold fr
      rw [unpackMantissa, BitVec.extractLsb'_toNat]
      simp [Nat.shiftRight_eq_div_pow]
    rw [hex, hfr]
    omega
  have frac_bound : ∀ b : B, (fr b).toNat < 2^52 := by
    intro b
    unfold fr
    rw [Float.Model.UnpackedFloat.unpackMantissa, BitVec.extractLsb'_toNat]
    simp [Nat.shiftRight_eq_div_pow]
    exact Nat.mod_lt _ (by native_decide : 0 < 2^52)
  intro a b ha hb hpa hpb
  let ba := a.toBits.toBitVec
  let bb := b.toBits.toBitVec
  have hfin_a : (unpack Float.Model.Format.binary64 ba).isFinite = true := by
    change (unpack Float.Model.Format.binary64 a.toBits.toBitVec).isFinite = true at ha
    simpa [ba] using ha
  have hfin_b : (unpack Float.Model.Format.binary64 bb).isFinite = true := by
    change (unpack Float.Model.Format.binary64 b.toBits.toBitVec).isFinite = true at hb
    simpa [bb] using hb
  have hpos_a0 : (0 : Float).toModel.lt a.toModel = true := by
    change (0 : Float).toModel.lt a.toModel = true at hpa
    exact hpa
  have hpos_b0 : (0 : Float).toModel.lt b.toModel = true := by
    change (0 : Float).toModel.lt b.toModel = true at hpb
    exact hpb
  have hmodelA : a.toModel.toBits = a.toBits := by rfl
  have hmodelB : b.toModel.toBits = b.toBits := by rfl
  have hpos_a : zeroUnpack.lt (unpack Float.Model.Format.binary64 ba) = true := by
    simpa [Float.Model.lt, Float.Model.unpack, hzeroModelBits, hmodelA, zeroUnpack, ba] using hpos_a0
  have hpos_b : zeroUnpack.lt (unpack Float.Model.Format.binary64 bb) = true := by
    simpa [Float.Model.lt, Float.Model.unpack, hzeroModelBits, hmodelB, zeroUnpack, bb] using hpos_b0
  obtain ⟨hsa, hma, hza⟩ := derive ba hfin_a hpos_a
  obtain ⟨hsb, hmb, hzb⟩ := derive bb hfin_b hpos_b
  have hza' : ex ba = 0#11 → (fr ba).toNat ≠ 0 := by
    intro he fzero
    apply hza
    constructor
    · exact he
    · apply BitVec.eq_of_toNat_eq
      simpa using fzero
  have hzb' : ex bb = 0#11 → (fr bb).toNat ≠ 0 := by
    intro he fzero
    apply hzb
    constructor
    · exact he
    · apply BitVec.eq_of_toNat_eq
      simpa using fzero
  have hcmp : (unpack Float.Model.Format.binary64 ba).lt
        (unpack Float.Model.Format.binary64 bb) = true ↔ lex ba bb :=
    lex_iff ba bb hsa hsb hma hmb hza' hzb'
  have hdec_a : (bits a).toNat = (ex ba).toNat * 2^52 + (fr ba).toNat := by
    change ba.toNat = (ex ba).toNat * 2^52 + (fr ba).toNat
    exact decomp ba hsa
  have hdec_b : (bits b).toNat = (ex bb).toNat * 2^52 + (fr bb).toNat := by
    change bb.toNat = (ex bb).toNat * 2^52 + (fr bb).toNat
    exact decomp bb hsb
  constructor
  · intro hltab
    have hcmpab : (unpack Float.Model.Format.binary64 ba).lt
        (unpack Float.Model.Format.binary64 bb) = true := by
      change a.toModel.lt b.toModel = true at hltab
      change (unpack Float.Model.Format.binary64 a.toBits.toBitVec).lt
          (unpack Float.Model.Format.binary64 b.toBits.toBitVec) = true at hltab
      simpa [ba, bb] using hltab
    have hl := hcmp.mp hcmpab
    unfold lex at hl
    rw [hdec_a, hdec_b]
    rcases hl with helt | ⟨heq, hfelt⟩
    · have hfa := frac_bound ba
      have hfb := frac_bound bb
      omega
    · have hfa := frac_bound ba
      have hfb := frac_bound bb
      omega
  · intro hbits
    rw [hdec_a, hdec_b] at hbits
    have hfa := frac_bound ba
    have hfb := frac_bound bb
    have hl : lex ba bb := by
      unfold lex
      by_cases helt : (ex ba).toNat < (ex bb).toNat
      · exact Or.inl helt
      · by_cases heq : (ex ba).toNat = (ex bb).toNat
        · right
          constructor
          · exact heq
          · omega
        · exfalso
          have hgt : (ex bb).toNat < (ex ba).toNat := by omega
          omega
    have hcmpab := hcmp.mpr hl
    change (unpack Float.Model.Format.binary64 a.toBits.toBitVec).lt
        (unpack Float.Model.Format.binary64 b.toBits.toBitVec) = true
    simpa [ba, bb] using hcmpab

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_NumericCompareAsSignedBitKey
