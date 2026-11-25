import TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A07

open TinylambOpt.ResearchGoals.Compression.Wire

/-! Per-goal notes:
- DelimitedInteger: true.  Structural induction on a fuel bound `n < 128^fuel`
  for the recursive ULEB128 decoder, keeping an arbitrary trailing frame.
- VarintCharge: true.  The decoded length and `ubytes` are both characterized
  by the single threshold `n < 128^k` (for `k > 0`), so they coincide.
- WidthCliff: true.  `ubytes n = max 1 ⌈width n / 7⌉` and `width n` is the bit
  length, so `ubytes n ≤ k ↔ n < 2^(7k) = 128^k`.
- SignedInverse: true.  Case split on `Int.ofNat` / `Int.negSucc`.
- SignedCliff: true.  For `k > 0`, `128^k = 2 * (128^k / 2)` and zigzag maps
  `[-q, q)` exactly onto `[0, 2q)` with `q = 128^k / 2`.
-/

/-! ## Width and the ULEB byte-count function -/

private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  unfold width
  simp [hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · rw [width_eq n hn]
    exact Nat.lt_log2_self

private theorem width_le_iff (n W : Nat) (hn : n ≠ 0) : width n ≤ W ↔ n < 2 ^ W := by
  rw [width_eq n hn]
  constructor
  · intro h
    exact (Nat.log2_lt hn).mp (by omega)
  · intro h
    have := (Nat.log2_lt hn).mpr h
    omega

private theorem ubytes_le_iff (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128 ^ k := by
  unfold ubytes
  constructor
  · intro h
    have hw7 : (width n + 6) / 7 ≤ k := (Nat.max_le.mp h).2
    have hw : width n ≤ 7 * k := by
      have := (Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)).mp hw7
      omega
    calc n < 2 ^ width n := lt_two_pow_width n
      _ ≤ 2 ^ (7 * k) := Nat.pow_le_pow_right (by decide : 0 < 2) hw
      _ = 128 ^ k := by
          have h128 : (128 : Nat) = 2 ^ 7 := by decide
          rw [h128, ← Nat.pow_mul]
  · intro h
    apply Nat.max_le.mpr
    constructor
    · omega
    · have hw : width n ≤ 7 * k := by
        by_cases hn : n = 0
        · subst hn
          simp [width]
        · have hlt : n < 2 ^ (7 * k) := by
            have h128 : (128 : Nat) = 2 ^ 7 := by decide
            rw [h128, ← Nat.pow_mul] at h
            exact h
          exact (width_le_iff n (7 * k) hn).mpr hlt
      rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)]
      omega

/-! ## DelimitedInteger -/

private theorem readU_uleb_aux (n fuel : Nat) (tail : Bytes) (hf : n < 128 ^ fuel) :
    readU (uleb n ++ tail) = some (n, tail) := by
  induction fuel generalizing n tail with
  | zero =>
      have h0 : 128 ^ 0 = 1 := by decide
      rw [h0] at hf
      have hn : n = 0 := by omega
      subst hn
      simp [uleb, readU]
  | succ fuel ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt, readU]
      · have hge : 128 ≤ n := Nat.not_lt.mp hlt
        have hmodlt : n % 128 < 128 := Nat.mod_lt n (by decide : 0 < 128)
        have hb1 : ¬ (128 + n % 128 < 128) := by omega
        have hb2 : 128 + n % 128 < 256 := by omega
        have hmod : (128 + n % 128) % 128 = n % 128 := by omega
        have hdiv : n / 128 < 128 ^ fuel := by
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
          calc n < 128 ^ (fuel + 1) := hf
            _ = 128 ^ fuel * 128 := by rw [Nat.pow_succ]
        have hrec := ih (n / 128) tail hdiv
        rw [uleb]
        simp [readU, hlt, hb1, hb2, hrec, hmod, Nat.mod_add_div]

theorem DelimitedInteger_sound : DelimitedInteger.Goal := by
  intro n tail
  have hf : n < 128 ^ (n + 1) := by
    calc n < 2 ^ n := Nat.lt_two_pow_self
      _ ≤ 128 ^ n := Nat.pow_le_pow_left (by decide : 2 ≤ 128) n
      _ ≤ 128 ^ n * 128 := Nat.le_mul_of_pos_right _ (by decide : 0 < 128)
      _ = 128 ^ (n + 1) := by rw [Nat.pow_succ]
  exact readU_uleb_aux n (n + 1) tail hf

/-! ## VarintCharge -/

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem uleb_len_succ (n : Nat) (h : ¬ n < 128) :
    (uleb n).length = 1 + (uleb (n / 128)).length := by
  rw [uleb]
  simp [h]
  omega

private theorem uleb_len_le_one (n : Nat) : (uleb n).length ≤ 1 ↔ n < 128 := by
  by_cases hn : n < 128
  · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
    rw [h1]
    simp [hn]
  · have hge : 128 ≤ n := Nat.not_lt.mp hn
    have hlen : 2 ≤ (uleb n).length := by
      rw [uleb_len_succ n hn]
      have := uleb_len_pos (n / 128)
      omega
    constructor
    · intro h; omega
    · intro h; omega

private theorem uleb_len_le_succ (n k : Nat) :
    (uleb n).length ≤ k + 1 ↔ n < 128 ^ (k + 1) := by
  induction k generalizing n with
  | zero =>
      simpa using uleb_len_le_one n
  | succ k ih =>
      by_cases hn : n < 128
      · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
        have hlen1 : (uleb n).length = 1 := by rw [h1]; simp
        constructor
        · intro _
          calc n < 128 := hn
            _ ≤ 128 ^ (k + 1 + 1) := by
                have hk1 : 1 ≤ k + 1 + 1 := by omega
                simpa using Nat.pow_le_pow_right (by decide : 0 < 128) hk1
        · intro _
          rw [hlen1]
          omega
      · have hge : 128 ≤ n := Nat.not_lt.mp hn
        rw [uleb_len_succ n hn]
        have hih := ih (n / 128)
        constructor
        · intro h
          have hd : (uleb (n / 128)).length ≤ k + 1 := by omega
          have hdiv : n / 128 < 128 ^ (k + 1) := hih.mp hd
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)] at hdiv
          calc n < 128 ^ (k + 1) * 128 := hdiv
            _ = 128 ^ (k + 1 + 1) := (Nat.pow_succ 128 (k + 1)).symm
        · intro h
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
            calc n < 128 ^ (k + 1 + 1) := h
              _ = 128 ^ (k + 1) * 128 := by rw [Nat.pow_succ]
          have hd := hih.mpr hdiv
          omega

private theorem uleb_len_le_iff (n k : Nat) (hk : 0 < k) :
    (uleb n).length ≤ k ↔ n < 128 ^ k := by
  obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  exact uleb_len_le_succ n j

theorem VarintCharge_sound : VarintCharge.Goal := by
  intro n
  have hlenpos : 0 < (uleb n).length := uleb_len_pos n
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · have hle : (uleb n).length ≤ ubytes n :=
      (uleb_len_le_iff n (ubytes n) hubpos).mpr
        ((ubytes_le_iff n (ubytes n) hubpos).mp (Nat.le_refl _))
    exact hle
  · exact (ubytes_le_iff n (uleb n).length hlenpos).mpr
      ((uleb_len_le_iff n (uleb n).length hlenpos).mp (Nat.le_refl _))

/-! ## WidthCliff -/

theorem WidthCliff_sound : WidthCliff.Goal := by
  intro n k hk
  exact ubytes_le_iff n k hk

/-! ## SignedInverse -/

theorem SignedInverse_sound : SignedInverse.Goal := by
  intro x
  cases x with
  | ofNat n =>
      simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

/-! ## SignedCliff -/

private theorem zig_lt_iff (x : Int) (q : Nat) :
    zig x < 2 * q ↔ -((q : Nat) : Int) ≤ x ∧ x < (q : Nat) := by
  cases x with
  | ofNat n =>
      simp [zig]
  | negSucc n =>
      simp [zig]
      omega

private theorem zig_lt_pow_iff (x : Int) (k : Nat) (hk : 0 < k) :
    zig x < 128 ^ k ↔
      -((128 ^ k / 2 : Nat) : Int) ≤ x ∧ x < ((128 ^ k / 2 : Nat) : Int) := by
  obtain ⟨j, hj⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  subst hj
  have hpow : 128 ^ (j + 1) = 2 * (64 * 128 ^ j) := by
    rw [Nat.pow_succ]
    omega
  have hdiv : 128 ^ (j + 1) / 2 = 64 * 128 ^ j := by
    rw [hpow]
    omega
  rw [hdiv, hpow]
  exact zig_lt_iff x (64 * 128 ^ j)

theorem SignedCliff_sound : SignedCliff.Goal := by
  intro x k hk
  rw [sbytes, ubytes_le_iff (zig x) k hk]
  exact zig_lt_pow_iff x k hk

end TinylambOpt.ResearchProofs.NewGoalsBatch.A07
