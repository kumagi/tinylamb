import TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A08

open TinylambOpt.ResearchGoals.Compression.Wire

/-! Per-goal notes:
- MaxWordBytes: true.  For `n < 2^64`, `Nat.log2 n ≤ 63`, hence
  `width n ≤ 64` and `ubytes n = max 1 ((width n+6)/7) ≤ (64+6)/7 = 10`
  (with the `n = 0` case giving `1`).
- PrefixFree: true.  ULEB128 is self-delimiting, so `uleb a` being a
  prefix of `uleb b` forces the two encodings to coincide; decoding
  `uleb b ++ tail` therefore yields `a = b` and the original tail.
- PackedInverse: true.  `pack` is little-endian `word`, whose base-`2^w`
  digits are exactly the (in-range) inputs; `fromLittle ∘ little` is
  `mod 256^k`, and the byte count covers all `w * length` bits.
- LocalRead: true.  The byte window `[i*w/8, ...)` covers all bits of
  lane `i`; the leading `shift` bits are divided off and the trailing
  bytes only contribute multiples of `2^w` to the quotient.
- PackedCharge: true.  `pack` produces `little _ k`, and `little` is a
  `map` over `List.range k`, so its length is definitionally `k`.
-/

/-- ULEB128 is self-delimiting: decoding `uleb n ++ tail` returns `n` and
the untouched tail. -/
private theorem readU_uleb_fuel (n fuel : Nat) (tail : Bytes) (hf : n < 128 ^ fuel) :
    readU (uleb n ++ tail) = some (n, tail) := by
  induction fuel generalizing n tail with
  | zero =>
      have : n = 0 := by omega
      subst this
      rw [uleb]
      simp [readU]
  | succ fuel ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt, readU]
      · have hn : 128 ≤ n := Nat.not_lt.mp hlt
        have hmod : 128 + n % 128 < 256 := by omega
        have hdiv : n / 128 < 128 ^ fuel := by
          rw [Nat.div_lt_iff_lt_mul (by omega)]
          simpa [Nat.pow_succ, Nat.mul_comm] using hf
        have hrec := ih (n / 128) ((128 + n % 128) :: tail) hdiv
        rw [uleb]
        simp only [hlt, ite_false, List.cons_append, readU]
        have hnotlt : ¬ (128 + n % 128 < 128) := by omega
        simp only [hnotlt, ite_false, hmod, ite_true]
        rw [hrec]
        simp only [Option.map_some]
        congr 1
        constructor
        · omega
        · rfl

private theorem readU_uleb (n : Nat) (tail : Bytes) :
    readU (uleb n ++ tail) = some (n, tail) := by
  have hf : n < 128 ^ (n + 1) := by
    have h2 : n < 128 ^ n := by
      calc n < 2 ^ n := Nat.lt_two_pow_self
        _ ≤ 128 ^ n := Nat.pow_le_pow_left (by omega) _
    calc n < 128 ^ n := h2
      _ ≤ 128 ^ n * 128 := Nat.le_mul_of_pos_right _ (by omega)
      _ = 128 ^ (n + 1) := by rw [Nat.pow_succ]
  exact readU_uleb_fuel n (n + 1) tail hf

theorem MaxWordBytes_sound : MaxWordBytes.Goal := by
  intro n hn
  unfold ubytes
  apply Nat.max_le.mpr
  constructor
  · omega
  · by_cases h0 : n = 0
    · subst h0
      simp [width]
      omega
    · have hlog : Nat.log2 n < 64 := (Nat.log2_lt h0).mpr hn
      have hw : width n ≤ 64 := by
        unfold width
        simp [h0]
        omega
      have hdiv : (width n + 6) / 7 ≤ (64 + 6) / 7 := Nat.div_le_div_right (by omega)
      have h64 : (64 + 6) / 7 = 10 := by decide
      omega

theorem PrefixFree_sound : PrefixFree.Goal := by
  intro a b tail hpre
  have hp : (uleb a) <+: (uleb b) := (List.isPrefixOf_iff_prefix).mp hpre
  obtain ⟨rest, hrest⟩ := hp
  have hb := readU_uleb b tail
  have ha : readU (uleb b ++ tail) = some (a, rest ++ tail) := by
    rw [hrest, List.append_assoc]
    exact readU_uleb a (rest ++ tail)
  have hpair : (b, tail) = (a, rest ++ tail) := by
    have : some (b, tail) = some (a, rest ++ tail) := by rw [← hb, ha]
    exact Option.some.inj this
  have hba : b = a := (Prod.ext_iff.mp hpair).1
  simpa [hba] using readU_uleb a tail

theorem PackedCharge_sound : PackedCharge.Goal := by
  intro w xs
  simp [pack, little]

end TinylambOpt.ResearchProofs.NewGoalsBatch.A08
