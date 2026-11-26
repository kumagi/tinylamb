import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-! Soundness of radix-precision monotonicity: refining the scale by one bit cannot change a
correctly guessed reciprocal window, so execution may use the finer scale for the same value. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_RadixPrecisionMonotonicity

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows.RadixPrecisionMonotonicity.Goal := by
  intro x d s hd hex
  unfold exactAt guess at hex ⊢
  unfold magic at hex ⊢
  let A := 2^s
  let q := (A + d - 1) / d
  let m := (2^(s+1) + d - 1) / d
  have hpow : 2^(s+1) = 2*A := by
    dsimp [A]
    rw [Nat.pow_succ]
    omega
  have hmul : ∀ t, 2^t ≤ ((2^t + d - 1)/d)*d := by
    intro t
    have hdivmod : ((2^t+d-1)/d)*d + (2^t+d-1)%d = 2^t+d-1 := by
      rw [Nat.mul_comm]
      exact Nat.div_add_mod (2^t+d-1) d
    have hmod : (2^t + d - 1) % d < d := Nat.mod_lt _ hd
    omega
  have hmA : A ≤ q*d := by
    dsimp [q, A]
    exact hmul s
  have hmprime : 2^(s+1) ≤ m*d := by
    dsimp [m]
    exact hmul (s+1)
  have hmagic_le : m ≤ 2*q := by
    change (2^(s+1)+d-1)/d ≤ 2*q
    rw [Nat.div_le_iff_le_mul_add_pred hd]
    rw [hpow]
    rw [show d*(2*q) = 2*(q*d) by ac_rfl]
    omega
  have hnum_le : x*m ≤ 2*(x*q) := by
    have h := Nat.mul_le_mul_left x hmagic_le
    calc
      x*m ≤ x*(2*q) := h
      _ = 2*(x*q) := by ac_rfl
  have hle : x*m / 2^(s+1) ≤ x*q / A := by
    rw [hpow]
    calc
      x*m / (2*A) ≤ (2*(x*q))/(2*A) := Nat.div_le_div_right hnum_le
      _ = x*q/A := by
        rw [Nat.mul_comm 2 A, Nat.mul_comm 2 (x*q)]
        exact Nat.mul_div_mul_right (x*q) A (by omega)
  have hxdiv : (x/d)*d ≤ x := by
    have hdivmod : (x/d)*d + x%d = x := by
      rw [Nat.mul_comm]
      exact Nat.div_add_mod x d
    omega
  have hnum_ge : (x/d)*2^(s+1) ≤ x*m := by
    have h1 : (x/d)*2^(s+1) ≤ (x/d)*(m*d) := Nat.mul_le_mul_left (x/d) hmprime
    calc
      (x/d)*2^(s+1) ≤ (x/d)*(m*d) := h1
      _ = m*((x/d)*d) := by ac_rfl
      _ ≤ m*x := Nat.mul_le_mul_left m hxdiv
      _ = x*m := by ac_rfl
  have hge : x/d ≤ x*m / 2^(s+1) := by
    rw [Nat.le_div_iff_mul_le (Nat.pow_pos (by omega : 0 < 2))]
    exact hnum_ge
  have hle' : x*m / 2^(s+1) ≤ x/d := by
    rw [← hex]
    simpa [A, q] using hle
  change x*m/2^(s+1) = x/d
  exact Nat.le_antisymm hle' hge

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_RadixPrecisionMonotonicity
