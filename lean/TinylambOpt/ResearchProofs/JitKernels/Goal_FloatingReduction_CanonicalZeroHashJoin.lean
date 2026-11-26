import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-! Canonical-zero hash-join soundness for binary64.

The theorem proves that IEEE-equal non-NaN inputs have identical bits after
positive-zero canonicalization: signed zeros both hash to the canonical zero
pattern, while finite and infinite equal values keep the same raw binary64
representation. This justifies treating canonicalized bits as a hash-join key
without changing the modeled SQL comparison result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_CanonicalZeroHashJoin

open Float.Model
open Float.Model.UnpackedFloat
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem sound : TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.CanonicalZeroHashJoin.Goal := by
  intro a b h
  let av := a.toModel.toBits.toBitVec
  let bv := b.toModel.toBits.toBitVec
  have model_ext {x y : Float.Model} (hxy : x.toBits = y.toBits) : x = y := by
    cases x with
    | mk xb xv =>
      cases y with
      | mk yb yv =>
        simp at hxy
        cases hxy
        rfl
  have packedZero_bits : (Float.Model.pack (.zero .positive)).toBits = 0 := by native_decide
  have packedZero_exp : unpackExponent (spec := Format.binary64)
      (Float.Model.pack (.zero .positive)).toBits.toBitVec = 0#11 := by native_decide
  have packedZero_man : unpackMantissa (spec := Format.binary64)
      (Float.Model.pack (.zero .positive)).toBits.toBitVec = 0#52 := by native_decide
  have packedZero_sign : unpackSign (spec := Format.binary64)
      (Float.Model.pack (.zero .positive)).toBits.toBitVec = 0#1 := by native_decide
  have zero_model : (0 : Float).toModel = Float.Model.pack (.zero .positive) := by
    apply model_ext
    have hz := (by native_decide : (0 : Float).toBits = 0)
    simpa [Float.toBits, packedZero_bits] using hz
  have zero_unpack : (0 : Float).toModel.unpack = .zero .positive := by
    rw [zero_model]
    rfl
  have unpack_pack_zero (s : Sign) : (Float.Model.pack (.zero s)).unpack = .zero s := by
    cases s <;> rfl
  have is_zero (x : Float)
      (hx0 : unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec = 0#11)
      (hxm : unpackMantissa (spec := Format.binary64) x.toModel.toBits.toBitVec = 0#52) :
      (x == 0) = true := by
    generalize hs : Sign.ofBitVec (unpackSign (spec := Format.binary64) x.toModel.toBits.toBitVec) = s
    cases s <;>
      simp [BEq.beq, Float.beq, zero_model, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
        Float.Model.unpack, UnpackedFloat.unpack, packedZero_exp, packedZero_man, packedZero_sign,
        instBEqOption.beq, hx0, hxm, hs, zero_unpack]
  have not_zero_inf (x : Float)
      (hx : unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec = 2047#11)
      (hxm : unpackMantissa (spec := Format.binary64) x.toModel.toBits.toBitVec = 0#52) :
      (x == 0) = false := by
    generalize hs : Sign.ofBitVec (unpackSign (spec := Format.binary64) x.toModel.toBits.toBitVec) = s
    cases s <;>
      simp [BEq.beq, Float.beq, zero_model, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
        Float.Model.unpack, UnpackedFloat.unpack, packedZero_exp, packedZero_man, packedZero_sign,
        instBEqOption.beq, hx, hxm, hs, zero_unpack]
  have not_zero_sub (x : Float)
      (hx0 : unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec = 0#11)
      (hxm : unpackMantissa (spec := Format.binary64) x.toModel.toBits.toBitVec ≠ 0#52) :
      (x == 0) = false := by
    generalize hs : Sign.ofBitVec (unpackSign (spec := Format.binary64) x.toModel.toBits.toBitVec) = s
    cases s <;>
      simp [BEq.beq, Float.beq, zero_model, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
        Float.Model.unpack, UnpackedFloat.unpack, packedZero_exp, packedZero_man, packedZero_sign,
        instBEqOption.beq, hx0, hxm, hs, zero_unpack]
  have not_zero_norm (x : Float)
      (hx : unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec ≠ 2047#11)
      (hx0 : unpackExponent (spec := Format.binary64) x.toModel.toBits.toBitVec ≠ 0#11) :
      (x == 0) = false := by
    generalize hs : Sign.ofBitVec (unpackSign (spec := Format.binary64) x.toModel.toBits.toBitVec) = s
    cases s <;>
      simp [BEq.beq, Float.beq, zero_model, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
        Float.Model.unpack, UnpackedFloat.unpack, packedZero_exp, packedZero_man, packedZero_sign,
        instBEqOption.beq, hx, hx0, hs, zero_unpack]
  have packComponents_unpack (bv : BitVec 64) :
      packComponents Format.binary64 (Sign.ofBitVec (unpackSign (spec := Format.binary64) bv))
        (unpackExponent (spec := Format.binary64) bv)
        (unpackMantissa (spec := Format.binary64) bv) = bv := by
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    change i < 64 at hi
    rw [packComponents]
    rw [BitVec.getLsbD_append]
    by_cases h52 : i < 52
    · simp [h52, unpackMantissa]
    · rw [BitVec.getLsbD_append]
      by_cases h63 : i < 63
      · have hidx : i - 52 < 11 := by omega
        have hadd : 52 + (i - 52) = i := by omega
        simp [h52, h63, hidx, unpackExponent, hadd]
      · have hidx63 : i = 63 := by omega
        simp [h52, h63, unpackSign, Sign.ofBitVec, Sign.toBitVec, hidx63]
        by_cases hs : BitVec.extractLsb' 63 1 bv = 0#1
        · have hsbit0 := congrArg (fun x : BitVec 1 => x.getLsbD 0) hs
          have hsbit : bv.getLsbD 63 = false := by
            simpa [BitVec.getLsbD_extractLsb'] using hsbit0
          simp [hs]
          exact hsbit
        · have hsb : bv.getLsbD 63 = true := by
            by_cases hbit : bv.getLsbD 63 = true
            · exact hbit
            · have hfalse' : bv.getLsbD 63 = false := by
                cases hb : bv.getLsbD 63 <;> simp_all
              exfalso
              apply hs
              apply BitVec.eq_of_getLsbD_eq
              intro j hj
              have hj0 : j = 0 := by omega
              subst j
              simpa [BitVec.getLsbD_extractLsb', hfalse']
          have hsbElem : bv[63] = true := by
            simpa [BitVec.getLsbD_eq_getElem (by omega)] using hsb
          simp [hs]
          exact hsb
  have rawEq (u v : BitVec 64)
      (hs : Sign.ofBitVec (unpackSign (spec := Format.binary64) u) =
        Sign.ofBitVec (unpackSign (spec := Format.binary64) v))
      (he : unpackExponent (spec := Format.binary64) u = unpackExponent (spec := Format.binary64) v)
      (hm : unpackMantissa (spec := Format.binary64) u = unpackMantissa (spec := Format.binary64) v) :
      u = v := by
    rw [← packComponents_unpack u, ← packComponents_unpack v]
    congr 1
  have bits_of_raw (hv : av = bv) : bits a = bits b := by
    simpa [bits, Float.toBits, av, bv] using congrArg UInt64.ofBitVec hv
  have hp52 : (2 : Nat) ^ 52 = 4503599627370496 := by native_decide
  have exponentEq
      (he : ((unpackExponent (spec := Format.binary64) av).toNat : Int) =
        ((unpackExponent (spec := Format.binary64) bv).toNat : Int)) :
      unpackExponent (spec := Format.binary64) av = unpackExponent (spec := Format.binary64) bv := by
    apply BitVec.eq_of_toNat_eq
    exact Int.ofNat_inj.mp he
  have normalMantEq
      (hm : 4503599627370496 ||| (unpackMantissa (spec := Format.binary64) av).toNat =
        4503599627370496 ||| (unpackMantissa (spec := Format.binary64) bv).toNat) :
      unpackMantissa (spec := Format.binary64) av = unpackMantissa (spec := Format.binary64) bv := by
    apply BitVec.eq_of_toNat_eq
    have hma_lt : (unpackMantissa (spec := Format.binary64) av).toNat < 2 ^ 52 :=
      BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
    have hmb_lt : (unpackMantissa (spec := Format.binary64) bv).toNat < 2 ^ 52 :=
      BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
    have hsuma : 4503599627370496 + (unpackMantissa (spec := Format.binary64) av).toNat =
        4503599627370496 ||| (unpackMantissa (spec := Format.binary64) av).toNat := by
      have := Nat.two_pow_add_eq_or_of_lt hma_lt 1
      simpa [hp52] using this
    have hsumb : 4503599627370496 + (unpackMantissa (spec := Format.binary64) bv).toNat =
        4503599627370496 ||| (unpackMantissa (spec := Format.binary64) bv).toNat := by
      have := Nat.two_pow_add_eq_or_of_lt hmb_lt 1
      simpa [hp52] using this
    exact Nat.add_left_cancel (hsuma.trans (hm.trans hsumb.symm))

  have infInf
      (haE : unpackExponent (spec := Format.binary64) av = 2047#11)
      (haM : unpackMantissa (spec := Format.binary64) av = 0#52)
      (hbE : unpackExponent (spec := Format.binary64) bv = 2047#11)
      (hbM : unpackMantissa (spec := Format.binary64) bv = 0#52) :
      bits (if a == 0 then 0 else a) = bits (if b == 0 then 0 else b) := by
    have ha0 := not_zero_inf a (by simpa [av] using haE) (by simpa [av] using haM)
    have hb0 := not_zero_inf b (by simpa [bv] using hbE) (by simpa [bv] using hbM)
    simp [bits, ha0, hb0]
    simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
      Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haE, haM, hbE, hbM, av, bv] at h
    generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) av) = sa at h
    generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) bv) = sb at h
    cases sa <;> cases sb
    case negative.negative =>
      apply bits_of_raw
      apply rawEq
      · exact hsa.trans hsb.symm
      · exact haE.trans hbE.symm
      · exact haM.trans hbM.symm
    case negative.positive =>
      exfalso
      exact (by native_decide : compare Sign.negative Sign.positive ≠ Ordering.eq) h
    case positive.negative =>
      exfalso
      exact (by native_decide : compare Sign.positive Sign.negative ≠ Ordering.eq) h
    case positive.positive =>
      apply bits_of_raw
      apply rawEq
      · exact hsa.trans hsb.symm
      · exact haE.trans hbE.symm
      · exact haM.trans hbM.symm

  have zeroZero
      (ha0 : unpackExponent (spec := Format.binary64) av = 0#11)
      (haM : unpackMantissa (spec := Format.binary64) av = 0#52)
      (hb0 : unpackExponent (spec := Format.binary64) bv = 0#11)
      (hbM : unpackMantissa (spec := Format.binary64) bv = 0#52) :
      bits (if a == 0 then 0 else a) = bits (if b == 0 then 0 else b) := by
    have haz := is_zero a (by simpa [av] using ha0) (by simpa [av] using haM)
    have hbz := is_zero b (by simpa [bv] using hb0) (by simpa [bv] using hbM)
    simp [bits, haz, hbz]

  have normNorm
      (haAll : unpackExponent (spec := Format.binary64) av ≠ 2047#11)
      (haZero : unpackExponent (spec := Format.binary64) av ≠ 0#11)
      (hbAll : unpackExponent (spec := Format.binary64) bv ≠ 2047#11)
      (hbZero : unpackExponent (spec := Format.binary64) bv ≠ 0#11) :
      bits (if a == 0 then 0 else a) = bits (if b == 0 then 0 else b) := by
    have ha0 := not_zero_norm a (by simpa [av] using haAll) (by simpa [av] using haZero)
    have hb0 := not_zero_norm b (by simpa [bv] using hbAll) (by simpa [bv] using hbZero)
    simp [bits, ha0, hb0]
    have hz : (Float.Model.pack (.zero .positive)).unpack = .zero .positive := unpack_pack_zero .positive
    simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
      Float.Model.unpack, UnpackedFloat.unpack, zero_model, hz,
      instBEqOption.beq, haAll, haZero, hbAll, hbZero, av, bv] at h
    generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) av) = sa at h
    generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) bv) = sb at h
    cases sa <;> cases sb <;> simp [hsa, hsb] at h
    case negative.negative =>
      apply bits_of_raw
      apply rawEq
      · exact hsa.trans hsb.symm
      · apply exponentEq
        simpa [av, bv] using h.1
      · apply normalMantEq
        simpa [av, bv] using h.2
    case positive.positive =>
      apply bits_of_raw
      apply rawEq
      · exact hsa.trans hsb.symm
      · apply exponentEq
        simpa [av, bv] using h.1
      · apply normalMantEq
        simpa [av, bv] using h.2

  have subSub
      (haZero : unpackExponent (spec := Format.binary64) av = 0#11)
      (haM : unpackMantissa (spec := Format.binary64) av ≠ 0#52)
      (hbZero : unpackExponent (spec := Format.binary64) bv = 0#11)
      (hbM : unpackMantissa (spec := Format.binary64) bv ≠ 0#52) :
      bits (if a == 0 then 0 else a) = bits (if b == 0 then 0 else b) := by
    have ha0 := not_zero_sub a (by simpa [av] using haZero) (by simpa [av] using haM)
    have hb0 := not_zero_sub b (by simpa [bv] using hbZero) (by simpa [bv] using hbM)
    simp [bits, ha0, hb0]
    simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
      Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haZero, haM, hbZero, hbM, av, bv] at h
    generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) av) = sa at h
    generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) bv) = sb at h
    cases sa <;> cases sb <;> simp [hsa, hsb] at h
    case negative.negative =>
      apply bits_of_raw
      apply rawEq
      · exact hsa.trans hsb.symm
      · exact haZero.trans hbZero.symm
      · simpa [av, bv] using h
    case positive.positive =>
      apply bits_of_raw
      apply rawEq
      · exact hsa.trans hsb.symm
      · exact haZero.trans hbZero.symm
      · simpa [av, bv] using h

  have normSubFalse
      (haAll : unpackExponent (spec := Format.binary64) av ≠ 2047#11)
      (haZero : unpackExponent (spec := Format.binary64) av ≠ 0#11)
      (hbZero : unpackExponent (spec := Format.binary64) bv = 0#11)
      (hbM : unpackMantissa (spec := Format.binary64) bv ≠ 0#52) : False := by
    simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
      Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, hbZero, hbM, av, bv] at h
    generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) av) = sa at h
    generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) bv) = sb at h
    cases sa <;> cases sb <;> simp [hsa, hsb] at h
    case negative.negative =>
      have hma_lt : (unpackMantissa (spec := Format.binary64) av).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
      have hmb_lt : (unpackMantissa (spec := Format.binary64) bv).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
      have hsuma : 4503599627370496 + (unpackMantissa (spec := Format.binary64) av).toNat =
          4503599627370496 ||| (unpackMantissa (spec := Format.binary64) av).toNat := by
        have := Nat.two_pow_add_eq_or_of_lt hma_lt 1
        simpa [hp52] using this
      have hmb_lt' : (unpackMantissa (spec := Format.binary64) bv).toNat < 4503599627370496 := by
        simpa [hp52] using hmb_lt
      have hm : 4503599627370496 ||| (unpackMantissa (spec := Format.binary64) av).toNat =
          (unpackMantissa (spec := Format.binary64) bv).toNat := by
        simpa [av, bv] using h.2
      have hge : 4503599627370496 ≤ (unpackMantissa (spec := Format.binary64) bv).toNat := by
        rw [← hm, ← hsuma]
        omega
      exact (Nat.not_lt_of_ge hge) hmb_lt' 
    case positive.positive =>
      have hma_lt : (unpackMantissa (spec := Format.binary64) av).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
      have hmb_lt : (unpackMantissa (spec := Format.binary64) bv).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
      have hsuma : 4503599627370496 + (unpackMantissa (spec := Format.binary64) av).toNat =
          4503599627370496 ||| (unpackMantissa (spec := Format.binary64) av).toNat := by
        have := Nat.two_pow_add_eq_or_of_lt hma_lt 1
        simpa [hp52] using this
      have hmb_lt' : (unpackMantissa (spec := Format.binary64) bv).toNat < 4503599627370496 := by
        simpa [hp52] using hmb_lt
      have hm : 4503599627370496 ||| (unpackMantissa (spec := Format.binary64) av).toNat =
          (unpackMantissa (spec := Format.binary64) bv).toNat := by
        simpa [av, bv] using h.2
      have hge : 4503599627370496 ≤ (unpackMantissa (spec := Format.binary64) bv).toNat := by
        rw [← hm, ← hsuma]
        omega
      exact (Nat.not_lt_of_ge hge) hmb_lt' 

  have subNormFalse
      (haZero : unpackExponent (spec := Format.binary64) av = 0#11)
      (haM : unpackMantissa (spec := Format.binary64) av ≠ 0#52)
      (hbAll : unpackExponent (spec := Format.binary64) bv ≠ 2047#11)
      (hbZero : unpackExponent (spec := Format.binary64) bv ≠ 0#11) : False := by
    simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
      Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haZero, haM, hbAll, hbZero, av, bv] at h
    generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) av) = sa at h
    generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) bv) = sb at h
    cases sa <;> cases sb <;> simp [hsa, hsb] at h
    case negative.negative =>
      have hma_lt : (unpackMantissa (spec := Format.binary64) av).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
      have hmb_lt : (unpackMantissa (spec := Format.binary64) bv).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
      have hsumb : 4503599627370496 + (unpackMantissa (spec := Format.binary64) bv).toNat =
          4503599627370496 ||| (unpackMantissa (spec := Format.binary64) bv).toNat := by
        have := Nat.two_pow_add_eq_or_of_lt hmb_lt 1
        simpa [hp52] using this
      have hma_lt' : (unpackMantissa (spec := Format.binary64) av).toNat < 4503599627370496 := by
        simpa [hp52] using hma_lt
      have hm : (unpackMantissa (spec := Format.binary64) av).toNat =
          4503599627370496 ||| (unpackMantissa (spec := Format.binary64) bv).toNat := by
        simpa [av, bv] using h.2
      have hge : 4503599627370496 ≤ (unpackMantissa (spec := Format.binary64) av).toNat := by
        rw [hm, ← hsumb]
        omega
      exact (Nat.not_lt_of_ge hge) hma_lt' 
    case positive.positive =>
      have hma_lt : (unpackMantissa (spec := Format.binary64) av).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
      have hmb_lt : (unpackMantissa (spec := Format.binary64) bv).toNat < 2 ^ 52 :=
        BitVec.toNat_lt_twoPow_of_le (Nat.le_refl 52)
      have hsumb : 4503599627370496 + (unpackMantissa (spec := Format.binary64) bv).toNat =
          4503599627370496 ||| (unpackMantissa (spec := Format.binary64) bv).toNat := by
        have := Nat.two_pow_add_eq_or_of_lt hmb_lt 1
        simpa [hp52] using this
      have hma_lt' : (unpackMantissa (spec := Format.binary64) av).toNat < 4503599627370496 := by
        simpa [hp52] using hma_lt
      have hm : (unpackMantissa (spec := Format.binary64) av).toNat =
          4503599627370496 ||| (unpackMantissa (spec := Format.binary64) bv).toNat := by
        simpa [av, bv] using h.2
      have hge : 4503599627370496 ≤ (unpackMantissa (spec := Format.binary64) av).toNat := by
        rw [hm, ← hsumb]
        omega
      exact (Nat.not_lt_of_ge hge) hma_lt' 

  by_cases haAll : unpackExponent (spec := Format.binary64) av = 2047#11
  · by_cases haM : unpackMantissa (spec := Format.binary64) av = 0#52
    · by_cases hbAll : unpackExponent (spec := Format.binary64) bv = 2047#11
      · by_cases hbM : unpackMantissa (spec := Format.binary64) bv = 0#52
        · exact infInf haAll haM hbAll hbM
        · exfalso
          simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
            Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haM, hbAll, hbM, av, bv] at h
      · by_cases hbZero : unpackExponent (spec := Format.binary64) bv = 0#11
        · by_cases hbM : unpackMantissa (spec := Format.binary64) bv = 0#52
          · exfalso
            simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
              Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haM, hbAll, hbZero, hbM, av, bv] at h
            all_goals
              generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
              generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
              cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
          · exfalso
            simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
              Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haM, hbAll, hbZero, hbM, av, bv] at h
            all_goals
              generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
              generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
              cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
        · exfalso
          simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
            Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haM, hbAll, hbZero, av, bv] at h
          all_goals
            generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
            generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
            cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
    · exfalso
      simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
        Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haM, av] at h
  · by_cases haZero : unpackExponent (spec := Format.binary64) av = 0#11
    · by_cases haM : unpackMantissa (spec := Format.binary64) av = 0#52
      · by_cases hbAll : unpackExponent (spec := Format.binary64) bv = 2047#11
        · by_cases hbM : unpackMantissa (spec := Format.binary64) bv = 0#52
          · exfalso
            simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
              Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, haM, hbAll, hbM, av, bv] at h
            all_goals
              generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
              generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
              cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
          · exfalso
            simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
              Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, haM, hbAll, hbM, av, bv] at h
        · by_cases hbZero : unpackExponent (spec := Format.binary64) bv = 0#11
          · by_cases hbM : unpackMantissa (spec := Format.binary64) bv = 0#52
            · exact zeroZero haZero haM hbZero hbM
            · exfalso
              simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
                Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, haM, hbAll, hbZero, hbM, av, bv] at h
              all_goals
                generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
                generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
                cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
          · exfalso
            simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
              Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, haM, hbAll, hbZero, av, bv] at h
            all_goals
              generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
              generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
              cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
      · by_cases hbAll : unpackExponent (spec := Format.binary64) bv = 2047#11
        · by_cases hbM : unpackMantissa (spec := Format.binary64) bv = 0#52
          · exfalso
            simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
              Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, haM, hbAll, hbM, av, bv] at h
            all_goals
              generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
              generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
              cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
          · exfalso
            simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
              Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, haM, hbAll, hbM, av, bv] at h
        · by_cases hbZero : unpackExponent (spec := Format.binary64) bv = 0#11
          · by_cases hbM : unpackMantissa (spec := Format.binary64) bv = 0#52
            · exfalso
              simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
                Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, haM, hbAll, hbZero, hbM, av, bv] at h
              all_goals
                generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
                generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
                cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
            · exact subSub haZero haM hbZero hbM
          · exact False.elim (subNormFalse haZero haM hbAll hbZero)
    · by_cases hbAll : unpackExponent (spec := Format.binary64) bv = 2047#11
      · by_cases hbM : unpackMantissa (spec := Format.binary64) bv = 0#52
        · exfalso
          simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
            Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, hbAll, hbM, av, bv] at h
          all_goals
            generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
            generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
            cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
        · exfalso
          simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
            Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, hbAll, hbM, av, bv] at h
      · by_cases hbZero : unpackExponent (spec := Format.binary64) bv = 0#11
        · by_cases hbM : unpackMantissa (spec := Format.binary64) bv = 0#52
          · exfalso
            simp [BEq.beq, Float.beq, Float.Model.beq, UnpackedFloat.beq, UnpackedFloat.compare,
              Float.Model.unpack, UnpackedFloat.unpack, instBEqOption.beq, haAll, haZero, hbAll, hbZero, hbM, av, bv] at h
            all_goals
              generalize hsa : Sign.ofBitVec (unpackSign (spec := Format.binary64) a.toModel.toBits.toBitVec) = sa at h
              generalize hsb : Sign.ofBitVec (unpackSign (spec := Format.binary64) b.toModel.toBits.toBitVec) = sb at h
              cases sa <;> cases sb <;> try simp [hsa, hsb] at h <;> contradiction
          · exact False.elim (normSubFalse haAll haZero hbZero hbM)
        · exact normNorm haAll haZero hbAll hbZero

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_CanonicalZeroHashJoin
