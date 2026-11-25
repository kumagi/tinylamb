import TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A07

open TinylambOpt.ResearchGoals.Compression.Wire

/-! Per-goal notes:
- DelimitedInteger: true. Induct on a fuel bound `n < 128^fuel` for the
  recursive ULEB128 decoder, carrying an arbitrary tail through unchanged.
- VarintCharge: true. Count base-128 digits structurally; both `(uleb n).length`
  and `ubytes n` satisfy `d ≤ k ↔ n < 128^k` for positive `k`.
- WidthCliff: true. `ubytes n = max 1 (ceil(width n / 7))` and `width n` is the
  bit length, so the comparison is exactly `n < 2^(7k) = 128^k`.
- SignedInverse: true. Case split on `Int.ofNat` / `Int.negSucc`.
- SignedCliff: true. For `k > 0`, `128^k = 2 * (128^k / 2)`; zigzag maps
  `[-q, q)` bijectively onto `[0, 2*q)` with `q = 128^k / 2`.
-/

private theorem ubytes_le_iff_aux (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128 ^ k := by
  unfold ubytes
  constructor
  · intro h
    have h1 : (width n + 6) / 7 ≤ k := Nat.le_trans (Nat.le_max_right 1 ((width n + 6) / 7)) h
    by_cases hn : n = 0
    · subst n
      simp
      exact Nat.pow_pos (by omega)
    · have hw : n < 2 ^ width n := by
        unfold width
        simp [hn]
        exact Nat.lt_log2_self
      have h3 : width n ≤ 7 * k := by omega
      calc n < 2 ^ width n := hw
        _ ≤ 2 ^ (7 * k) := Nat.pow_le_pow_right (by omega) h3
        _ = (2 ^ 7) ^ k := by rw [Nat.pow_mul]
        _ = 128 ^ k := by
          have h128 : (2 : Nat) ^ 7 = 128 := by decide
          rw [h128]
  · intro h
    apply Nat.max_le.mpr
    constructor
    · omega
    · rw [Nat.div_le_iff_le_mul_add_pred (by omega)]
      by_cases hn : n = 0
      · simp [width, hn]
      · have hlt : n < 2 ^ (7 * k) := by
          have h128 : 128 ^ k = 2 ^ (7 * k) := by
            have h' : (128 : Nat) = 2 ^ 7 := by decide
            rw [h', ← Nat.pow_mul]
          rw [h128] at h
          exact h
        have hw : width n ≤ 7 * k := by
          unfold width
          simp [hn]
          have hlog : Nat.log2 n < 7 * k := (Nat.log2_lt hn).mpr hlt
          omega
        omega

private theorem readU_uleb_fuel (n fuel : Nat) (tail : Bytes) (hf : n < 128 ^ fuel) :
    readU (uleb n ++ tail) = some (n, tail) := by
  induction fuel generalizing n tail with
  | zero =>
      have hn0 : n = 0 := by omega
      subst n
      simp [uleb, readU]
  | succ fuel ih =>
      by_cases hlt : n < 128
      · rw [uleb, ite_eq_left hlt]
        simp [readU, hlt]
      · have hmod : 128 + n % 128 < 256 := by omega
        have hdiv : n / 128 < 128 ^ fuel := by
          rw [Nat.div_lt_iff_lt_mul (by omega : 0 < 128)]
          simpa [Nat.pow_succ, Nat.mul_comm] using hf
        have hrec := ih (n / 128) tail hdiv
        rw [uleb, ite_eq_right hlt]
        simp only [List.cons_append, readU]
        have hnotlt : ¬ (128 + n % 128 < 128) := by omega
        rw [ite_eq_right hnotlt, ite_eq_left hmod]
        rw [hrec]
        simp only [Option.map_some]
        congr 1
        apply Prod.ext
        · omega
        · rfl

theorem DelimitedInteger_sound : DelimitedInteger.Goal := by
  intro n tail
  have hf : n < 128 ^ (n + 1) := by
    have h2 : n < 128 ^ n := by
      calc n < 2 ^ n := Nat.lt_two_pow_self
        _ ≤ 128 ^ n := Nat.pow_le_pow_left (by omega) _
    calc n < 128 ^ n := h2
      _ ≤ 128 ^ n * 128 := Nat.le_mul_of_pos_right _ (by omega)
      _ = 128 ^ (n + 1) := by rw [Nat.pow_succ]
  exact readU_uleb_fuel n (n + 1) tail hf

private def digits128 (n : Nat) : Nat :=
  if n < 128 then 1 else 1 + digits128 (n / 128)
termination_by n
decreasing_by exact Nat.div_lt_self (by omega) (by decide)

private theorem digits128_pos (n : Nat) : 0 < digits128 n := by
  rw [digits128]
  split <;> omega

private theorem digits128_le_iff_succ (n k : Nat) : digits128 n ≤ k + 1 ↔ n < 128 ^ (k + 1) := by
  induction k generalizing n with
  | zero =>
      rw [digits128]
      by_cases hlt : n < 128
      · rw [ite_eq_left hlt]
        simp [hlt]
      · rw [ite_eq_right hlt]
        simp only [Nat.zero_add, Nat.pow_one]
        constructor
        · intro h
          have hdpos := digits128_pos (n / 128)
          omega
        · intro h
          omega
  | succ k ih =>
      rw [digits128]
      by_cases hlt : n < 128
      · rw [ite_eq_left hlt]
        have hpos : 0 < 128 ^ (k + 1) := Nat.pow_pos (by omega)
        have hle : 128 ≤ 128 ^ (k + 1 + 1) := by
          simpa [Nat.pow_succ, Nat.mul_comm] using (Nat.le_mul_of_pos_right 128 hpos)
        constructor
        · intro _; exact Nat.lt_of_lt_of_le hlt hle
        · intro _; omega
      · rw [ite_eq_right hlt]
        constructor
        · intro h
          have hd : digits128 (n / 128) ≤ k + 1 := by omega
          have hdiv : n / 128 < 128 ^ (k + 1) := (ih (n / 128)).mp hd
          have hnlt : n < 128 ^ (k + 1) * 128 :=
            (Nat.div_lt_iff_lt_mul (by omega : 0 < 128)).mp hdiv
          simpa [Nat.pow_succ] using hnlt
        · intro h
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by omega : 0 < 128)]
            simpa [Nat.pow_succ] using h
          have hd : digits128 (n / 128) ≤ k + 1 := (ih (n / 128)).mpr hdiv
          omega

private theorem digits128_le_iff (n k : Nat) (hk : 0 < k) : digits128 n ≤ k ↔ n < 128 ^ k := by
  obtain ⟨j, hj⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  subst hj
  exact digits128_le_iff_succ n j

private theorem uleb_length_eq (n : Nat) : (uleb n).length = digits128 n := by
  rw [uleb, digits128]
  by_cases hlt : n < 128
  · rw [ite_eq_left hlt]
    simp [hlt]
  · rw [ite_eq_right hlt]
    simp [hlt]
    have ih := uleb_length_eq (n / 128)
    omega
termination_by n
decreasing_by exact Nat.div_lt_self (by omega) (by decide)

theorem VarintCharge_sound : VarintCharge.Goal := by
  intro n
  have hlen : 0 < (uleb n).length := by rw [uleb_length_eq]; exact digits128_pos n
  have hub : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · rw [uleb_length_eq]
    exact (digits128_le_iff n (ubytes n) hub).mpr
      ((ubytes_le_iff_aux n (ubytes n) hub).mp (Nat.le_refl _))
  · exact (ubytes_le_iff_aux n (uleb n).length hlen).mpr
      ((digits128_le_iff n (uleb n).length hlen).mp (by
        rw [uleb_length_eq]
        exact Nat.le_refl _))

theorem WidthCliff_sound : WidthCliff.Goal := by
  intro n k hk
  exact ubytes_le_iff_aux n k hk

theorem SignedInverse_sound : SignedInverse.Goal := by
  intro x
  cases x with
  | ofNat n =>
      simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem zig_lt_iff (x : Int) (q : Nat) :
    zig x < 2 * q ↔ -((q : Nat) : Int) ≤ x ∧ x < (q : Nat) := by
  cases x with
  | ofNat n =>
      simp [zig]
  | negSucc n =>
      simp [zig]
      omega

private theorem zig_lt_pow_iff (x : Int) (k : Nat) (hk : 0 < k) :
    zig x < 128 ^ k ↔ -((128 ^ k / 2 : Nat) : Int) ≤ x ∧ x < ((128 ^ k / 2 : Nat) : Int) := by
  obtain ⟨j, hj⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  subst hj
  have hpow : 128 ^ (j + 1) = 2 * (64 * 128 ^ j) := by
    rw [Nat.pow_succ]
    have h128 : (128 : Nat) = 2 * 64 := by decide
    rw [h128]
    ac_rfl
  have hdiv : 128 ^ (j + 1) / 2 = 64 * 128 ^ j := by
    rw [hpow]
    exact Nat.mul_div_right (64 * 128 ^ j) (by decide)
  rw [hdiv, hpow]
  exact zig_lt_iff x (64 * 128 ^ j)

theorem SignedCliff_sound : SignedCliff.Goal := by
  intro x k hk
  rw [sbytes, ubytes_le_iff_aux (zig x) k hk]
  exact zig_lt_pow_iff x k hk

end TinylambOpt.ResearchProofs.NewGoalsBatch.A07
