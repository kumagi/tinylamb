import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Residue-sensitive correction test: with the round-up multiplier m = ceil(2^s / d) and
its error excess d*m - 2^s, the guess x*m/2^s equals x/d exactly when the accumulated
error (x/d)*excess + (x%d)*m stays below 2^s. Rewriting that condition yields
x*excess < (d - x%d)*2^s, so the kernel may omit the correction step exactly when this
boundary test holds; integer-division results in SQL are unchanged either way.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_ResidueSensitiveNoCorrection
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

theorem sound : ResidueSensitiveNoCorrection.Goal := by
  intro x d s hd
  have hP : 0 < 2^s := Nat.pow_pos (by omega)
  have hd1 : d * (x / d) + x % d = x := Nat.div_add_mod x d
  have hmod := Nat.mod_lt x hd
  have hdm := magic_eq d s hd
  have e1 : x * excess d s = (x / d) * d * excess d s + x % d * excess d s := by
    have h := congrArg (· * excess d s) hd1
    rw [Nat.add_mul, Nat.mul_comm d (x / d)] at h
    exact h.symm
  have hrmd : magic d s * d = 2^s + excess d s := by
    rw [Nat.mul_comm (magic d s) d, hdm]
  have hq : d * ((x / d) * excess d s) = (x / d) * d * excess d s := by
    rw [Nat.mul_comm d ((x / d) * excess d s), Nat.mul_assoc, Nat.mul_comm (excess d s) d,
      ← Nat.mul_assoc]
  have hrm : d * (x % d * magic d s) = x % d * 2^s + x % d * excess d s := by
    rw [Nat.mul_comm d (x % d * magic d s), Nat.mul_assoc, hrmd, Nat.mul_add]
  have e2 : d * ((x / d) * excess d s + x % d * magic d s) =
      (x / d) * d * excess d s + (x % d * 2^s + x % d * excess d s) := by
    rw [Nat.mul_add, hq, hrm]
  have e3 : d * 2^s = x % d * 2^s + (d - x % d) * 2^s := by
    have hle : x % d ≤ d := Nat.le_of_lt hmod
    have h := congrArg (· * 2^s) (Nat.sub_add_cancel hle)
    rw [Nat.add_mul] at h
    omega
  constructor
  · intro hex
    have ht := (char x d s hd).mp hex
    have h4 : (x / d) * d * excess d s + (x % d * 2^s + x % d * excess d s) < d * 2^s := by
      have h5 : d * ((x / d) * excess d s + x % d * magic d s) < d * 2^s :=
        Nat.mul_lt_mul_of_pos_left ht hd
      rw [e2] at h5
      exact h5
    omega
  · intro h
    have h4 : (x / d) * d * excess d s + (x % d * 2^s + x % d * excess d s) < d * 2^s := by
      omega
    have h5 : d * ((x / d) * excess d s + x % d * magic d s) < d * 2^s := by
      rw [e2]; exact h4
    apply (char x d s hd).mpr
    exact Nat.lt_of_not_le fun hge => by
      have h7 : d * 2^s ≤ d * ((x / d) * excess d s + x % d * magic d s) :=
        Nat.mul_le_mul_left d hge
      omega

end TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_ResidueSensitiveNoCorrection
