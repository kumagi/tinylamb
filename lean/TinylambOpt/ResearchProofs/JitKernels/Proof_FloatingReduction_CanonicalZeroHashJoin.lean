import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-!
Establishes `CanonicalZeroHashJoin.Goal`: when two `Float` values are IEEE-equal
(`a == b`), normalizing every IEEE zero to the positive-zero bit pattern before
hashing yields congruent bits. `Float` is a structure over `Float.Model`, a valid
`UInt64` bit pattern; `==` unfolds through `Float.beq` / `Model.beq` to
`UnpackedFloat.compare` on the inductive unpacked form. Case analysis on the
unpacked shape (`eq_dichotomy`) shows the IEEE-equality classes are singletons
except for `{+0.0, -0.0}` — which the normalization branch folds to one pattern —
and `NaN`, which is never `==`-equal and is the only family of bit patterns where
the roundtrip `pack ∘ unpack` loses information (`pack_unpack` needs
`isNaN = false`). For every other IEEE-equal pair the observable bits are
reconstructible from the unpacked value, so the two normalized patterns agree
bitwise. This justifies folding `-0.0` into `0.0` before computing hash-join
keys. All helpers are private; core `Init` + `Std` only, no Mathlib.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 100000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_CanonicalZeroHashJoin
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open Float.Model UnpackedFloat Format Sign

/-- Reconstructing a one-bit sign from its bit reading is the identity. -/
private theorem signRT (v : BitVec 1) : Sign.toBitVec (Sign.ofBitVec v) = v := by
  have h : v = 0#1 ∨ v = 1#1 := by revert v; decide
  rcases h with rfl | rfl <;> rfl

private theorem hbias (b : BitVec 64) :
    ((((b.extractLsb' 52 11).toNat : Int) - (1023 + 52)) + 1023 + 52).toNat
      = (b.extractLsb' 52 11).toNat := by omega

private theorem hlog (b : BitVec 64) : (1#1 ++ b.extractLsb' 0 52).toNat.log2 = 52 := by
  rw [BitVec.toNat_append_eq_mul_add]
  have h1n : ((1#1 : BitVec 1).toNat) = 1 := rfl
  have hlt := BitVec.isLt (b.extractLsb' 0 52)
  rw [h1n, Nat.one_mul, Nat.log2_eq_iff (by omega : (2 : Nat)^52 + (b.extractLsb' 0 52).toNat ≠ 0)]
  omega

/-- The `pack ∘ unpack` roundtrip is the identity on every non-`NaN` bit pattern:
the unpacked form stores the full payload except for `NaN`, whose mantissa bits
collapse to a single value. -/
private theorem pack_unpack (b : BitVec 64) (hn : (UnpackedFloat.unpack binary64 b).isNaN = false) :
    (UnpackedFloat.pack binary64 (UnpackedFloat.unpack binary64 b) : BitVec 64) = b := by
  have nest : UnpackedFloat.unpack binary64 b =
      (if b.extractLsb' 52 11 = (-1 : BitVec 11) then
        if b.extractLsb' 0 52 = (0 : BitVec 52) then .infinity (Sign.ofBitVec (b.extractLsb' 63 1))
        else .notANumber
      else if b.extractLsb' 52 11 = (0 : BitVec 11) then
        if h : b.extractLsb' 0 52 = (0 : BitVec 52) then .zero (Sign.ofBitVec (b.extractLsb' 63 1))
        else .finite (Sign.ofBitVec (b.extractLsb' 63 1)) (b.extractLsb' 0 52).toNat
          (((b.extractLsb' 52 11).toNat : Int) - (1023 + 52) + 1)
          (BitVec.toNat_pos_of_ne_zero h)
      else .finite (Sign.ofBitVec (b.extractLsb' 63 1)) (1#1 ++ b.extractLsb' 0 52).toNat
        (((b.extractLsb' 52 11).toNat : Int) - (1023 + 52))
        (by have h : (1#1 ++ b.extractLsb' 0 52).toNat
              = (1#1 : BitVec 1).toNat * 2^52 + (b.extractLsb' 0 52).toNat
            := BitVec.toNat_append_eq_mul_add _ _
            have h1 : ((1#1 : BitVec 1).toNat) = 1 := rfl
            rw [h, h1]; omega)) := rfl
  by_cases h1 : b.extractLsb' 52 11 = (-1 : BitVec 11)
  · by_cases h2 : b.extractLsb' 0 52 = (0 : BitVec 52)
    · -- ±infinity
      have hu : UnpackedFloat.unpack binary64 b = .infinity (Sign.ofBitVec (b.extractLsb' 63 1)) := by
        rw [nest]; rw [ite_eq_left h1, ite_eq_left h2]
      rw [hu]
      show (packedInfinity binary64 _ : BitVec 64) = b
      show (Sign.toBitVec (Sign.ofBitVec (b.extractLsb' 63 1)) ++ (-1 : BitVec 11) ++ (0 : BitVec 52) : BitVec 64) = b
      rw [signRT]
      apply BitVec.eq_of_toNat_eq
      have h1t : b.toNat >>> 52 % 2^11 = 2047 := by
        have := congrArg BitVec.toNat h1
        simp only [BitVec.extractLsb'_toNat] at this
        simpa using this
      have h2t : b.toNat % 2^52 = 0 := by
        have := congrArg BitVec.toNat h2
        simp only [BitVec.extractLsb'_toNat, Nat.shiftRight_zero] at this
        simpa using this
      simp only [BitVec.toNat_append_eq_mul_add, BitVec.extractLsb'_toNat, Nat.shiftRight_zero,
        show ((-1 : BitVec 11).toNat) = 2047 from rfl, show ((0 : BitVec 52).toNat) = 0 from rfl,
        Nat.mul_zero, Nat.add_zero, Nat.zero_add]
      omega
    · -- NaN: excluded by hypothesis
      have hu : UnpackedFloat.unpack binary64 b = .notANumber := by
        rw [nest]; rw [ite_eq_left h1, ite_eq_right h2]
      rw [hu] at hn
      simp only [UnpackedFloat.isNaN] at hn
      exact absurd hn (by decide)
  · by_cases h2 : b.extractLsb' 52 11 = (0 : BitVec 11)
    · by_cases h3 : b.extractLsb' 0 52 = (0 : BitVec 52)
      · -- signed zero
        have hu : UnpackedFloat.unpack binary64 b = .zero (Sign.ofBitVec (b.extractLsb' 63 1)) := by
          rw [nest]; rw [ite_eq_right h1, ite_eq_left h2, dite_eq_left h3]
        rw [hu]
        show (packedZero binary64 _ : BitVec 64) = b
        show (Sign.toBitVec (Sign.ofBitVec (b.extractLsb' 63 1)) ++ (0 : BitVec 11) ++ (0 : BitVec 52) : BitVec 64) = b
        rw [signRT]
        apply BitVec.eq_of_toNat_eq
        have h2t : b.toNat >>> 52 % 2^11 = 0 := by
          have := congrArg BitVec.toNat h2
          simp only [BitVec.extractLsb'_toNat] at this
          simpa using this
        have h3t : b.toNat % 2^52 = 0 := by
          have := congrArg BitVec.toNat h3
          simp only [BitVec.extractLsb'_toNat, Nat.shiftRight_zero] at this
          simpa using this
        simp only [BitVec.toNat_append_eq_mul_add, BitVec.extractLsb'_toNat, Nat.shiftRight_zero,
          show ((0 : BitVec 52).toNat) = 0 from rfl, show ((0 : BitVec 11).toNat) = 0 from rfl,
          Nat.mul_zero, Nat.add_zero, Nat.zero_add, Nat.zero_mul]
        omega
      · -- subnormal
        have hne : 0 < (b.extractLsb' 0 52).toNat := BitVec.toNat_pos_of_ne_zero h3
        have hu : UnpackedFloat.unpack binary64 b =
            .finite (Sign.ofBitVec (b.extractLsb' 63 1)) (b.extractLsb' 0 52).toNat (-1074) hne := by
          rw [nest]; rw [ite_eq_right h1, ite_eq_left h2, dite_eq_right h3]
          rw [h2]
          show UnpackedFloat.finite (Sign.ofBitVec (b.extractLsb' 63 1)) (b.extractLsb' 0 52).toNat
              ((((0 : BitVec 11).toNat : Int) - (1023 + 52)) + 1) _ = _
          show UnpackedFloat.finite (Sign.ofBitVec (b.extractLsb' 63 1)) (b.extractLsb' 0 52).toNat
              ((0 : Int) - (1023 + 52) + 1) _ = _
          rfl
        rw [hu]
        have h53 : (b.extractLsb' 0 52).toNat.log2 + 1 ≠ 53 := by
          intro hc
          have h0 : (b.extractLsb' 0 52).toNat ≠ 0 := by
            intro hc0; exact h3 (BitVec.eq_of_toNat_eq (by simpa using hc0))
          have hlt := BitVec.isLt (b.extractLsb' 0 52)
          have hle := (Nat.log2_eq_iff h0).mp (by omega : (b.extractLsb' 0 52).toNat.log2 = 52)
          omega
        show (if (2 : Nat)^11 ≤ 2 then
            packComponents binary64 (Sign.ofBitVec (b.extractLsb' 63 1)) (-1 : BitVec 11) (0 : BitVec 52)
          else if (b.extractLsb' 0 52).toNat.log2 + 1 = 53 then
            packComponents binary64 (Sign.ofBitVec (b.extractLsb' 63 1))
              (BitVec.ofNat 11 ((((-1074 : Int) + 1023 + 52)).toNat)) (BitVec.ofNat 52 (b.extractLsb' 0 52).toNat)
          else
            packComponents binary64 (Sign.ofBitVec (b.extractLsb' 63 1)) (0 : BitVec 11)
              (BitVec.ofNat 52 (b.extractLsb' 0 52).toNat) : BitVec 64) = b
        rw [ite_eq_right (by decide : ¬ ((2 : Nat)^11 ≤ 2)), ite_eq_right h53]
        show (Sign.toBitVec (Sign.ofBitVec (b.extractLsb' 63 1)) ++ (0 : BitVec 11)
            ++ BitVec.ofNat 52 (b.extractLsb' 0 52).toNat : BitVec 64) = b
        rw [signRT]
        apply BitVec.eq_of_toNat_eq
        have h2t : b.toNat >>> 52 % 2^11 = 0 := by
          have := congrArg BitVec.toNat h2
          simp only [BitVec.extractLsb'_toNat] at this
          simpa using this
        have h0lt : b.toNat % 2^52 < 2^52 := by
          have := BitVec.isLt (b.extractLsb' 0 52)
          simpa [BitVec.extractLsb'_toNat, Nat.shiftRight_zero] using this
        simp only [BitVec.toNat_append_eq_mul_add, BitVec.toNat_ofNat, BitVec.extractLsb'_toNat,
          Nat.shiftRight_zero, show ((0 : BitVec 11).toNat) = 0 from rfl, Nat.mul_zero, Nat.add_zero,
          Nat.zero_add]
        omega
    · -- normal (finite)
      have hu : UnpackedFloat.unpack binary64 b =
          .finite (Sign.ofBitVec (b.extractLsb' 63 1))
            (1#1 ++ b.extractLsb' 0 52).toNat (((b.extractLsb' 52 11).toNat : Int) - (1023 + 52))
            (by simp) := by
        rw [nest]; rw [ite_eq_right h1, ite_eq_right h2]
      rw [hu]
      have h1t : (b.extractLsb' 52 11).toNat ≠ 2047 := by
        intro hc; apply h1
        apply BitVec.eq_of_toNat_eq
        rw [hc, show ((-1 : BitVec 11).toNat) = 2047 from rfl]
      have hg1 : ¬ ((2 : Nat)^11 ≤ ((b.extractLsb' 52 11).toNat + 1)) := by
        intro hc
        have hlt : (b.extractLsb' 52 11).toNat < 2048 := by
          simpa using BitVec.isLt (b.extractLsb' 52 11)
        omega
      show (if (2 : Nat)^11 ≤ ((((b.extractLsb' 52 11).toNat : Int) - (1023 + 52)) + 1023 + 52).toNat + 1 then
          packComponents binary64 (Sign.ofBitVec (b.extractLsb' 63 1)) (-1 : BitVec 11) (0 : BitVec 52)
        else if (1#1 ++ b.extractLsb' 0 52).toNat.log2 + 1 = 53 then
          packComponents binary64 (Sign.ofBitVec (b.extractLsb' 63 1))
            (BitVec.ofNat 11 ((((b.extractLsb' 52 11).toNat : Int) - (1023 + 52)) + 1023 + 52).toNat)
            (BitVec.ofNat 52 (1#1 ++ b.extractLsb' 0 52).toNat)
        else
          packComponents binary64 (Sign.ofBitVec (b.extractLsb' 63 1)) (0 : BitVec 11)
            (BitVec.ofNat 52 (1#1 ++ b.extractLsb' 0 52).toNat) : BitVec 64) = b
      rw [hbias b]
      rw [ite_eq_right hg1]
      rw [hlog b, ite_eq_left (rfl : (52 : Nat) + 1 = 53)]
      show (Sign.toBitVec (Sign.ofBitVec (b.extractLsb' 63 1))
          ++ BitVec.ofNat 11 (b.extractLsb' 52 11).toNat
          ++ BitVec.ofNat 52 (1#1 ++ b.extractLsb' 0 52).toNat : BitVec 64) = b
      rw [signRT]
      apply BitVec.eq_of_toNat_eq
      have h52lt : b.toNat >>> 52 % 2^11 < 2^11 := by
        have := BitVec.isLt (b.extractLsb' 52 11); simpa [BitVec.extractLsb'_toNat] using this
      have h0lt : b.toNat % 2^52 < 2^52 := by
        have := BitVec.isLt (b.extractLsb' 0 52)
        simpa [BitVec.extractLsb'_toNat, Nat.shiftRight_zero] using this
      simp only [BitVec.toNat_append_eq_mul_add, BitVec.toNat_ofNat, BitVec.extractLsb'_toNat,
        Nat.shiftRight_zero, show ((1#1 : BitVec 1).toNat) = 1 from rfl, Nat.one_mul, Nat.add_zero,
        Nat.zero_add]
      omega

/-- `compareOfLessAndEq` returns `eq` only for equal arguments. -/
private theorem cmpEq {α} {x y : α} [LT α] [Decidable (x < y)] [DecidableEq α]
    (h : compareOfLessAndEq x y = Ordering.eq) : x = y := by
  by_cases hlt : x < y
  · have h1 : compareOfLessAndEq x y = Ordering.lt := by
      show (if x < y then Ordering.lt else if x = y then Ordering.eq else Ordering.gt) = _
      rw [ite_eq_left hlt]
    rw [h1] at h; exact absurd h (by decide)
  · have h1 : compareOfLessAndEq x y = (if x = y then Ordering.eq else Ordering.gt) := by
      show (if x < y then Ordering.lt else if x = y then Ordering.eq else Ordering.gt) = _
      rw [ite_eq_right hlt]
    rw [h1] at h
    by_cases heq : x = y
    · exact heq
    · rw [ite_eq_right heq] at h; exact absurd h (by decide)

private theorem then_eq {a b : Ordering} :
    a.then b = Ordering.eq ↔ a = Ordering.eq ∧ b = Ordering.eq := by
  cases a <;> cases b <;> decide

/-- Two positive finite unpacked values are IEEE-equal only when identical. -/
private theorem posPosEq {m m' : Nat} {e e' : Int} {hm hm'}
    (h : UnpackedFloat.beq (.finite .positive m e hm) (.finite .positive m' e' hm') = true) :
    m = m' ∧ e = e' := by
  have h2 : (compareOfLessAndEq e e').then (compareOfLessAndEq m m') = Ordering.eq := by
    cases hO : (compareOfLessAndEq e e').then (compareOfLessAndEq m m') with
    | eq => rfl
    | lt =>
        have h1 : (some ((compareOfLessAndEq e e').then (compareOfLessAndEq m m')) ==
            some Ordering.eq) = true := h
        rw [hO] at h1; exact absurd h1 (by decide)
    | gt =>
        have h1 : (some ((compareOfLessAndEq e e').then (compareOfLessAndEq m m')) ==
            some Ordering.eq) = true := h
        rw [hO] at h1; exact absurd h1 (by decide)
  have ⟨h1, h2'⟩ := then_eq.mp h2
  exact ⟨cmpEq h2', cmpEq h1⟩

/-- Two negative finite unpacked values are IEEE-equal only when identical. -/
private theorem negNegEq {m m' : Nat} {e e' : Int} {hm hm'}
    (h : UnpackedFloat.beq (.finite .negative m e hm) (.finite .negative m' e' hm') = true) :
    m = m' ∧ e = e' := by
  have h2 : (compareOfLessAndEq e e').then (compareOfLessAndEq m m') = Ordering.eq := by
    cases hO : (compareOfLessAndEq e e').then (compareOfLessAndEq m m') with
    | eq => rfl
    | lt =>
        have h1 : (((compareOfLessAndEq e e').then (compareOfLessAndEq m m')).swap ==
            Ordering.eq) = true := h
        rw [hO] at h1; exact absurd h1 (by decide)
    | gt =>
        have h1 : (((compareOfLessAndEq e e').then (compareOfLessAndEq m m')).swap ==
            Ordering.eq) = true := h
        rw [hO] at h1; exact absurd h1 (by decide)
  have ⟨h1, h2'⟩ := then_eq.mp h2
  exact ⟨cmpEq h2', cmpEq h1⟩

/-- IEEE equality between unpacked values pairs them identically, except that any
two signed zeros are equal and `NaN` never compares equal. -/
private theorem eq_dichotomy {u v : UnpackedFloat} (h : u.beq v = true) :
    (∃ s t : Sign, u = .zero s ∧ v = .zero t) ∨ (u = v ∧ u.isNaN = false) := by
  cases u with
  | notANumber => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
  | infinity su =>
      cases su with
      | positive =>
          cases v with
          | notANumber => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | infinity sv =>
              cases sv with
              | positive => exact Or.inr ⟨rfl, rfl⟩
              | negative => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | zero sv => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | finite sv m e hm => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
      | negative =>
          cases v with
          | notANumber => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | infinity sv =>
              cases sv with
              | negative => exact Or.inr ⟨rfl, rfl⟩
              | positive => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | zero sv => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | finite sv m e hm => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
  | zero su =>
      cases v with
      | notANumber => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
      | infinity sv => cases sv <;> exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
      | zero sv => exact Or.inl ⟨su, sv, rfl, rfl⟩
      | finite sv m e hm => cases sv <;> exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
  | finite su m e hm =>
      cases su with
      | positive =>
          cases v with
          | notANumber => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | infinity sv => cases sv <;> exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | zero sv => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | finite sv m' e' hm' =>
              cases sv with
              | positive =>
                  have ⟨hmeq, heeq⟩ := posPosEq h
                  subst hmeq; subst heeq
                  exact Or.inr ⟨congrArg (fun p => UnpackedFloat.finite Sign.positive m e p)
                    (Subsingleton.elim hm hm'), rfl⟩
              | negative => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
      | negative =>
          cases v with
          | notANumber => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | infinity sv => cases sv <;> exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | zero sv => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])
          | finite sv m' e' hm' =>
              cases sv with
              | negative =>
                  have ⟨hmeq, heeq⟩ := negNegEq h
                  subst hmeq; subst heeq
                  exact Or.inr ⟨congrArg (fun p => UnpackedFloat.finite Sign.negative m e p)
                    (Subsingleton.elim hm hm'), rfl⟩
              | positive => exact absurd h (by simp [UnpackedFloat.beq, UnpackedFloat.compare])

/-- Both signed zeros are IEEE-equal to positive zero. -/
private theorem zero_beq (s : Sign) :
    UnpackedFloat.beq (UnpackedFloat.zero s) (UnpackedFloat.zero Sign.positive) = true := by
  cases s <;> rfl

theorem sound : CanonicalZeroHashJoin.Goal := by
  intro a b hab
  have hbeq : a.toModel.unpack.beq b.toModel.unpack = true := hab
  by_cases hza : (a == (0 : Float)) = true
  · by_cases hzb : (b == (0 : Float)) = true
    · -- Both sides normalize to `0`.
      rw [ite_eq_left hza, ite_eq_left hzb]
    · -- `a` is an IEEE zero but `b` is not: impossible under `a == b`.
      have hza' : a.toModel.unpack.beq (UnpackedFloat.zero Sign.positive) = true := hza
      rcases eq_dichotomy hbeq with ⟨s, t, ha, ht⟩ | ⟨huv, _⟩
      · have hb0 : (b == (0 : Float)) = true := by
          show UnpackedFloat.beq b.toModel.unpack (UnpackedFloat.zero Sign.positive) = true
          rw [ht]; exact zero_beq t
        contradiction
      · have hb0 : (b == (0 : Float)) = true := by
          show UnpackedFloat.beq b.toModel.unpack (UnpackedFloat.zero Sign.positive) = true
          rw [← huv]; exact hza'
        contradiction
  · by_cases hzb : (b == (0 : Float)) = true
    · -- `b` is an IEEE zero but `a` is not: impossible under `a == b`.
      have hzb' : b.toModel.unpack.beq (UnpackedFloat.zero Sign.positive) = true := hzb
      rcases eq_dichotomy hbeq with ⟨s, t, ha, ht⟩ | ⟨huv, _⟩
      · have ha0 : (a == (0 : Float)) = true := by
          show UnpackedFloat.beq a.toModel.unpack (UnpackedFloat.zero Sign.positive) = true
          rw [ha]; exact zero_beq s
        contradiction
      · have ha0 : (a == (0 : Float)) = true := by
          show UnpackedFloat.beq a.toModel.unpack (UnpackedFloat.zero Sign.positive) = true
          rw [huv]; exact hzb'
        contradiction
    · -- Neither side normalizes: IEEE equality forces identical bit patterns.
      rw [ite_eq_right hza]
      rw [ite_eq_right hzb]
      rcases eq_dichotomy hbeq with ⟨s, t, ha, ht⟩ | ⟨huv, hn⟩
      · have ha0 : (a == (0 : Float)) = true := by
          show UnpackedFloat.beq a.toModel.unpack (UnpackedFloat.zero Sign.positive) = true
          rw [ha]; exact zero_beq s
        contradiction
      · show a.toModel.toBits = b.toModel.toBits
        apply UInt64.toBitVec_inj.mp
        have hna : (UnpackedFloat.unpack binary64 (a.toModel.toBits.toBitVec)).isNaN = false := hn
        have hnb' : b.toModel.unpack.isNaN = false := by rw [← huv]; exact hn
        have hnb : (UnpackedFloat.unpack binary64 (b.toModel.toBits.toBitVec)).isNaN = false := hnb'
        rw [← pack_unpack (a.toModel.toBits.toBitVec) hna,
          ← pack_unpack (b.toModel.toBits.toBitVec) hnb]
        exact congrArg (pack binary64) huv

end TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_CanonicalZeroHashJoin
