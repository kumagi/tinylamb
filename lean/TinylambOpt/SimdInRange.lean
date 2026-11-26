import Std.Tactic

/-!
Unsigned view of a two's-complement `W`-bit value: `z mod 2^W`.

If `-M < z < M`, then `z mod M` is either `z` (non-negative) or `z + M`.

Named results include `emod_eq_self_or_add`, `modulus_eq_twice_half`,
`simd_in_range_sound`.
-/

namespace TinylambOpt.SimdInRange

/-
# Signed range check as one unsigned comparison

Lowering of `lo <= x && x <= hi` for a `W`-bit two's-complement lane:

    U(x - lo) <= U(hi - lo)          -- both subtractions wrap modulo 2^W

where `U(z) = z mod 2^W` is taken in `[0, 2^W)`.  Reference and optimized
computations are modelled on mathematical `Int`; the wrapping subtraction is
`Int.emod` by `M = 2^W`.

The endpoints and input must fit the signed lane, with lo ≤ hi. Even the
full signed interval works: if x < lo, x + 2^W is strictly greater than hi.
The intended LLVM subtractions use wrapping arithmetic with no nsw/nuw.
-/

/-- Unsigned view of a two's-complement `W`-bit value: `z mod 2^W`. -/
def unsigned (M z : Int) : Int := z % M

/-- If `-M < z < M`, then `z mod M` is either `z` (non-negative) or `z + M`. -/
theorem emod_eq_self_or_add (z M : Int) (_hM : 0 < M) (hl : -M < z) (hu : z < M) :
    z % M = z ∨ z % M = z + M := by
  by_cases hz : 0 ≤ z
  · exact Or.inl (Int.emod_eq_of_lt hz hu)
  · right
    have hnonneg : 0 ≤ z + M := by omega
    have hlt : z + M < M := by omega
    have h1 : (z + M) % M = z + M := Int.emod_eq_of_lt hnonneg hlt
    have h2 : (z + 1 * M) % M = z % M := Int.add_mul_emod_self_right z 1 M
    have h2' : (z + M) % M = z % M := by
      simp
    omega

/-- For `W >= 2`, `2^W = 2 * 2^(W-1)`. -/
theorem modulus_eq_twice_half (W : Nat) (hW : 2 ≤ W) :
    (2 : Int) ^ W = 2 * (2 : Int) ^ (W - 1) := by
  have hp := Int.pow_succ (2 : Int) (W - 1)
  have hs : (W - 1).succ = W := by omega
  rw [hs] at hp
  rw [hp]
  omega

/-- **Reference vs optimized**: `lo <= x <= hi` (signed) is equivalent to
    `unsigned M (x - lo) <= unsigned M (hi - lo)`, under the explicit
    hypotheses documented above. -/
theorem simd_in_range_sound (W : Nat) (hW : 2 ≤ W)
    (lo hi x : Int)
    (loLow : -((2 : Int) ^ (W - 1)) ≤ lo)
    (loHigh : lo ≤ (2 : Int) ^ (W - 1) - 1)
    (_hiLow : -((2 : Int) ^ (W - 1)) ≤ hi)
    (hiHigh : hi ≤ (2 : Int) ^ (W - 1) - 1)
    (xLow : -((2 : Int) ^ (W - 1)) ≤ x)
    (xHigh : x ≤ (2 : Int) ^ (W - 1) - 1)
    (order : lo ≤ hi) :
    (lo ≤ x ∧ x ≤ hi) ↔
      unsigned ((2 : Int) ^ W) (x - lo) ≤ unsigned ((2 : Int) ^ W) (hi - lo) := by
  let M : Int := (2 : Int) ^ W
  let H : Int := (2 : Int) ^ (W - 1)
  have hM : M = 2 * H := by
    dsimp [M, H]
    exact modulus_eq_twice_half W hW
  have hHpos : 0 < H := by
    dsimp [H]
    exact Int.pow_pos (show (0 : Int) < 2 by decide)
  have hMpos : 0 < M := by omega
  have hMne : M ≠ 0 := by omega
  have hd0 : 0 ≤ hi - lo := by omega
  have hdM : hi - lo < M := by omega
  change (lo ≤ x ∧ x ≤ hi) ↔ unsigned M (x - lo) ≤ unsigned M (hi - lo)
  have hdu : unsigned M (hi - lo) = hi - lo := by
    exact Int.emod_eq_of_lt hd0 hdM
  constructor
  · rintro ⟨hxl, hxh⟩
    have hx0 : 0 ≤ x - lo := by omega
    have hxM : x - lo < M := by omega
    have hxu : unsigned M (x - lo) = x - lo := Int.emod_eq_of_lt hx0 hxM
    rw [hxu, hdu]
    omega
  · intro hcmp
    have hzlow : -M < x - lo := by omega
    have hzhigh : x - lo < M := by omega
    have hcases : (x - lo) % M = x - lo ∨ (x - lo) % M = (x - lo) + M :=
      emod_eq_self_or_add (x - lo) M hMpos hzlow hzhigh
    have hdu' : (hi - lo) % M = hi - lo := by
      exact Int.emod_eq_of_lt hd0 hdM
    have hcmp' : (x - lo) % M ≤ hi - lo := by
      simpa [unsigned, hdu'] using hcmp
    have hmod_nonneg : 0 ≤ (x - lo) % M := Int.emod_nonneg _ hMne
    rcases hcases with hself | hwrap
    · rw [hself] at hcmp'
      constructor <;> omega
    · rw [hwrap] at hcmp'
      constructor <;> omega

/-- Concrete INT64 JIT operation, including MIN..MAX. Both subtractions
    are the ordinary wrapping BitVec instructions. -/
theorem int64_range_lowering (lo hi x : BitVec 64) (ordered : lo.sle hi = true) :
    (lo.sle x && x.sle hi) = (x - lo).ule (hi - lo) := by
  have wrap (a b : BitVec 64) : unsigned (2^64) (a.toInt-b.toInt) = ((a-b).toNat : Int) := by
    rw [← BitVec.toInt_emod_two_pow, BitVec.toInt_sub]
    exact (Int.bmod_emod ..).symm
  have spec := simd_in_range_sound 64 (by decide) lo.toInt hi.toInt x.toInt
    lo.le_toInt BitVec.toInt_le hi.le_toInt BitVec.toInt_le x.le_toInt BitVec.toInt_le
    (BitVec.sle_iff_toInt_le.mp ordered)
  rw [wrap,wrap] at spec
  apply Bool.eq_iff_iff.mpr
  simpa only [Bool.and_eq_true, BitVec.sle_iff_toInt_le,BitVec.ule_iff_toNat_le,Int.ofNat_le] using spec

end TinylambOpt.SimdInRange

-- C++ references to this model or its proof.
-- expression/bytecode.cpp
-- expression/bytecode.hpp
-- expression/jit.cpp
