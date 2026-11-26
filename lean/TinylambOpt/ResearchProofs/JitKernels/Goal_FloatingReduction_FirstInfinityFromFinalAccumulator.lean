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
  by_cases hb : b = 0#1
  · simp [hb, Sign.ofBitVec, Sign.toBitVec]
  · have hb1 : b = 1#1 := by
      apply BitVec.eq_of_toNat_eq
      have hlt : b.toNat < 2 := by simpa using b.isLt
      have hne : b.toNat ≠ 0 := by
        intro h0
        apply hb
        apply BitVec.eq_of_toNat_eq
        simpa [h0]
      change b.toNat = 1
      omega
    subst b
    rfl

theorem unpackSign_pc64 (s : Sign) (e : BitVec 11) (m : BitVec 52) :
    unpackSign (spec := Format.binary64) (packComponents Format.binary64 s e m) = s.toBitVec := by
  cases s <;> simp only [unpackSign, packComponents, Sign.toBitVec] <;>
    simp only [BitVec.extractLsb'_cast, BitVec.append_assoc] <;>
      rw [BitVec.extractLsb'_append_eq_left]

theorem unpackExp_append (s : Sign) (e : BitVec 11) (m : BitVec 52) :
    unpackExponent (spec := Format.binary64) (s.toBitVec ++ e ++ m) = e := by
  cases s <;> simp only [unpackExponent, Sign.toBitVec] <;>
    simp only [BitVec.extractLsb'_cast, BitVec.append_assoc] <;>
      rw [BitVec.extractLsb'_append_eq_of_add_le (by omega : 52 + 11 ≤ 63),
        BitVec.extractLsb'_append_eq_left]

theorem unpackMan_append (s : Sign) (e : BitVec 11) (m : BitVec 52) :
    unpackMantissa (spec := Format.binary64) (s.toBitVec ++ e ++ m) = m := by
  cases s <;> simp only [unpackMantissa, Sign.toBitVec] <;>
    simp only [BitVec.extractLsb'_cast, BitVec.append_assoc] <;>
      rw [BitVec.extractLsb'_append_eq_of_add_le (by omega : 0 + 52 ≤ 63),
        BitVec.extractLsb'_append_eq_of_add_le (by omega : 0 + 52 ≤ 52),
        BitVec.extractLsb'_eq_self]

theorem unpackSign_append (s : Sign) (e : BitVec 11) (m : BitVec 52) :
    unpackSign (spec := Format.binary64) (s.toBitVec ++ e ++ m) = s.toBitVec := by
  cases s <;> simp only [unpackSign, Sign.toBitVec] <;>
    simp only [BitVec.extractLsb'_cast, BitVec.append_assoc] <;>
      rw [BitVec.extractLsb'_append_eq_left]

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


/-! With finite inputs an overflow to infinity is absorbing: the unpacked adder
returns infinity for `inf + finite`, and a sum of two finite values never produces
NaN. Therefore the first infinity in the scan is still present in the final
accumulator, and conversely the final accumulator is itself one of the scanned
prefixes. Skipping the prefix scan and keeping only the sticky infinity flag is
hence safe for a finite-input reduction. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_FirstInfinityFromFinalAccumulator

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open TinylambOpt.ResearchProofs.JitKernels.FloatingReductionCommon
open Float.Model
open Float.Model.UnpackedFloat

private theorem unpackFinite_of_mem {x : Float} (hx : finite x = true) :
    x.toModel.unpack.isFinite = true := by
  simpa [_root_.TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.finite, Float.isFinite, Float.Model.isFinite] using hx

private theorem foldl_unpack_inf_of_inf (acc : Float) (xs : List Float) (s : Sign)
    (hs : acc.toModel.unpack = .infinity s) (hxs : ∀ x ∈ xs, finite x) :
    (xs.foldl (fun a x => a + x) acc).toModel.unpack = .infinity s := by
  induction xs generalizing acc with
  | nil => simpa
  | cons x xs ih =>
      rw [List.foldl_cons]
      have hs' : (acc + x).toModel.unpack = .infinity s :=
        add_unpack_inf_of_inf_of_finite acc x s hs (unpackFinite_of_mem (hxs x (by simp)))
      apply ih (acc + x) hs'
      intro y hy
      exact hxs y (by simp [hy])

private theorem foldl_inf_of_inf (acc : Float) (xs : List Float)
    (hacc : acc.isInf = true) (hxs : ∀ x ∈ xs, finite x) :
    (xs.foldl (fun a x => a + x) acc).isInf = true := by
  obtain ⟨s, hs⟩ : ∃ s, acc.toModel.unpack = .infinity s := by
    simp only [Float.isInf, Float.Model.isInf, UnpackedFloat.isInf] at hacc
    cases hu : acc.toModel.unpack with
    | infinity s => exact ⟨s, rfl⟩
    | _ => simp [hu] at hacc
  simp only [Float.isInf, Float.Model.isInf]
  rw [foldl_unpack_inf_of_inf acc xs s hs hxs]
  rfl

private theorem aux (acc : Float) (xs : List Float) (hnan : acc.isNaN = false)
    (hxs : ∀ x ∈ xs, finite x) :
    ((xs.scanl (fun a x => a + x) acc).findIdx? Float.isInf).isSome =
      (xs.foldl (fun a x => a + x) acc).isInf := by
  induction xs generalizing acc with
  | nil =>
      by_cases hp : Float.isInf acc = true <;> simp [hp]
  | cons x xs ih =>
      rw [List.scanl_cons, List.findIdx?_cons, List.foldl_cons]
      by_cases hinf : Float.isInf acc
      · obtain ⟨s, hs⟩ : ∃ s, acc.toModel.unpack = .infinity s := by
          simp only [Float.isInf, Float.Model.isInf, UnpackedFloat.isInf] at hinf
          cases hu : acc.toModel.unpack with
          | infinity s => exact ⟨s, rfl⟩
          | _ => simp [hu] at hinf
        have htail := foldl_inf_of_inf (acc + x) xs
          (add_inf_of_inf_of_finite acc x s hs (unpackFinite_of_mem (hxs x (by simp))))
          (by intro y hy; exact hxs y (by simp [hy]))
        simp [hinf, htail]
      · have hnn : (acc + x).isNaN = false :=
          add_not_nan_of_not_nan_of_finite acc x hnan (unpackFinite_of_mem (hxs x (by simp)))
        have ih' := ih (acc + x) hnn (by intro y hy; exact hxs y (by simp [hy]))
        simp [hinf, ih']

theorem sound : FirstInfinityFromFinalAccumulator.Goal := by
  intro xs s hs hxs
  unfold firstInf total
  exact aux s xs (finite_not_nan hs) hxs

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_FirstInfinityFromFinalAccumulator
