import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Radix/precision monotonicity: moving the generated reciprocal from shift `s` to `s+1`
at most doubles both the multiplier and its error, so a dividend whose guess was exact
at shift `s` stays exact at shift `s+1`. Raising precision never introduces a spurious
quotient in the SQL division kernel; no re-verification of already-exact rows is needed.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_RadixPrecisionMonotonicity
open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

private theorem magic_ge (d s : Nat) (hd : 0 < d) : 2^s ≤ d * magic d s := by
  have hmod := Nat.div_add_mod (2^s + d - 1) d
  have hlt := Nat.mod_lt (2^s + d - 1) hd
  unfold magic at hmod ⊢
  omega

private theorem magic_eq (d s : Nat) (hd : 0 < d) :
    d * magic d s = 2^s + excess d s := by
  have h := magic_ge d s hd
  unfold excess
  omega

/-- Core characterization: the guess is exact iff the accumulated error stays below 2^s. -/
private theorem char (x d s : Nat) (hd : 0 < d) :
    guess x d s = x / d ↔ (x / d) * excess d s + (x % d) * magic d s < 2^s := by
  have hP : 0 < 2^s := Nat.pow_pos (by omega)
  have hd1 : d * (x / d) + x % d = x := Nat.div_add_mod x d
  have hdm := magic_eq d s hd
  have hkey : x * magic d s =
      (x / d) * 2^s + ((x / d) * excess d s + (x % d) * magic d s) := by
    have e0 : x * magic d s = d * (x / d) * magic d s + x % d * magic d s := by
      have h := congrArg (· * magic d s) hd1
      rw [Nat.add_mul] at h
      exact h.symm
    have e1 : d * (x / d) * magic d s = (x / d) * (d * magic d s) := by
      rw [Nat.mul_comm d (x / d), Nat.mul_assoc]
    have e2 : (x / d) * (d * magic d s) = (x / d) * (2^s + excess d s) :=
      congrArg ((x / d) * ·) hdm
    have e3 : (x / d) * (2^s + excess d s) =
        (x / d) * 2^s + (x / d) * excess d s := Nat.mul_add _ _ _
    rw [e0, e1, e2, e3]
    omega
  have hgq : guess x d s =
      ((x / d) * excess d s + (x % d) * magic d s) / 2^s + x / d := by
    show x * magic d s / 2^s = _
    rw [hkey, Nat.mul_comm (x / d) (2^s),
      Nat.add_comm (2^s * (x / d)) (x / d * excess d s + x % d * magic d s)]
    exact Nat.add_mul_div_left _ _ hP
  constructor
  · intro heq
    rw [hgq] at heq
    have hz : ((x / d) * excess d s + (x % d) * magic d s) / 2^s = 0 := by omega
    rw [Nat.div_eq_zero_iff] at hz
    cases hz with
    | inl hz0 => omega
    | inr hlt => exact hlt
  · intro hlt
    rw [hgq]
    have hz : ((x / d) * excess d s + (x % d) * magic d s) / 2^s = 0 :=
      (Nat.div_eq_zero_iff).mpr (Or.inr hlt)
    omega

private theorem div_le_of_lt (a b k : Nat) (hb : 0 < b) (h : a < b * (k + 1)) : a / b ≤ k := by
  have hkey : ¬ (k + 1 ≤ a / b) := by
    intro hk1
    have hmul := (Nat.le_div_iff_mul_le hb).mp hk1
    have hmul' : b * (k + 1) ≤ a := by
      rw [Nat.mul_comm]
      exact hmul
    omega
  omega

theorem sound : RadixPrecisionMonotonicity.Goal := by
  intro x d s hd h
  have hge := magic_ge d s hd
  have h3a : d * (2 * magic d s) = 2 * (d * magic d s) := by
    rw [← Nat.mul_assoc, Nat.mul_comm d 2, Nat.mul_assoc]
  have hm : magic d (s + 1) ≤ 2 * magic d s := by
    have h1 : 2^(s + 1) ≤ 2 * (d * magic d s) := calc
      2^(s + 1) = 2^s * 2 := Nat.pow_succ 2 s
      _ ≤ (d * magic d s) * 2 := Nat.mul_le_mul_right 2 hge
      _ = 2 * (d * magic d s) := Nat.mul_comm _ _
    have h2 : 2^(s + 1) + d - 1 < 2 * (d * magic d s) + d := by omega
    have h3 : d * (2 * magic d s + 1) = 2 * (d * magic d s) + d := by
      rw [Nat.mul_add, h3a, Nat.mul_one]
    have h4 : 2^(s + 1) + d - 1 < d * (2 * magic d s + 1) := by rw [h3]; exact h2
    exact div_le_of_lt (2^(s + 1) + d - 1) d (2 * magic d s) hd h4
  have hB : d * magic d (s + 1) ≤ 2 * (d * magic d s) := by
    rw [← h3a]
    exact Nat.mul_le_mul_left d hm
  have E1 := magic_eq d s hd
  have E2 := magic_eq d (s + 1) hd
  have hE2 : 2 * (d * magic d s) = 2^(s + 1) + 2 * excess d s :=
    (congrArg (2 * ·) E1).trans
      (by rw [Nat.mul_add, Nat.mul_comm 2 (2^s), ← Nat.pow_succ])
  have h6 : d * magic d (s + 1) ≤ 2^(s + 1) + 2 * excess d s := by
    rw [← hE2]
    exact hB
  have hE : excess d (s + 1) ≤ 2 * excess d s := by omega
  have ht := (char x d s hd).mp h
  have hq : (x / d) * excess d (s + 1) ≤ 2 * ((x / d) * excess d s) := by
    have h2 : (x / d) * (2 * excess d s) = 2 * ((x / d) * excess d s) := by
      rw [← Nat.mul_assoc, Nat.mul_comm (x / d) 2, Nat.mul_assoc]
    rw [← h2]
    exact Nat.mul_le_mul_left (x / d) hE
  have hr : x % d * magic d (s + 1) ≤ 2 * (x % d * magic d s) := by
    have h2 : x % d * (2 * magic d s) = 2 * (x % d * magic d s) := by
      rw [← Nat.mul_assoc, Nat.mul_comm (x % d) 2, Nat.mul_assoc]
    rw [← h2]
    exact Nat.mul_le_mul_left (x % d) hm
  have hsum : (x / d) * excess d (s + 1) + x % d * magic d (s + 1) ≤
      2 * ((x / d) * excess d s + x % d * magic d s) := by
    rw [Nat.mul_add]
    exact Nat.add_le_add hq hr
  have hP : 0 < 2^s := Nat.pow_pos (by omega)
  have hlt : 2 * ((x / d) * excess d s + x % d * magic d s) < 2 * 2^s :=
    Nat.mul_lt_mul_of_pos_left ht (by omega)
  have h2P : 2 * 2^s = 2^(s + 1) := by rw [Nat.mul_comm, Nat.pow_succ]
  have hfin : (x / d) * excess d (s + 1) + x % d * magic d (s + 1) < 2^(s + 1) := by
    rw [← h2P]
    exact Nat.lt_of_le_of_lt hsum hlt
  exact (char x d (s + 1) hd).mpr hfin

end TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_RadixPrecisionMonotonicity
