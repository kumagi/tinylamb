import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-!
A rounded-up reciprocal produces the exact quotient precisely when the product
of its error and the dividend stays below the next quotient boundary. The
residue-dependent bound is needed by SQL division kernels that omit the
correction step for profiled dividend windows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_ResidueSensitiveNoCorrection

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem sound : ResidueSensitiveNoCorrection.Goal := by
  intro x d s hd
  let N := 2 ^ s
  let m := magic d s
  let e := excess d s
  let q := x / d
  let r := x % d
  have hN : 0 < N := by
    dsimp [N]
    exact Nat.two_pow_pos s
  have hm : m = (N + d - 1) / d := by simp [m, magic, N]
  have hN_le : N ≤ d * m := by
    have hdiv := Nat.div_add_mod (N + d - 1) d
    have hlt := Nat.mod_lt (N + d - 1) hd
    rw [← hm] at hdiv
    omega
  have he : e = d * m - N := by simp [e, excess, m, N]
  have hdm_eq : d * m = N + e := by omega
  have hx : d * q + r = x := by simp [q, r, Nat.div_add_mod]
  have hxm : x * m = q * N + (q * e + r * m) := by
    rw [← hx]
    calc
      (d * q + r) * m = q * (d * m) + r * m := by
        simp [Nat.add_mul, Nat.mul_assoc, Nat.mul_left_comm]
      _ = q * (N + e) + r * m := by rw [hdm_eq]
      _ = q * N + (q * e + r * m) := by
        simp [Nat.mul_add]
        omega
  have hguess : guess x d s = q + (q * e + r * m) / N := by
    unfold guess
    change (x * m) / N = q + (q * e + r * m) / N
    rw [hxm]
    rw [Nat.mul_comm q N, Nat.add_comm]
    rw [Nat.add_mul_div_left _ _ hN]
    omega
  have hexact_iff : exactAt x d s ↔ q * e + r * m < N := by
    unfold exactAt
    rw [hguess]
    change q + (q * e + r * m) / N = q ↔ q * e + r * m < N
    constructor
    · intro h
      have ht : (q * e + r * m) / N = 0 := by omega
      rcases Nat.div_eq_zero_iff.mp ht with hzero | hlt
      · omega
      · exact hlt
    · intro hlt
      have hz : (q * e + r * m) / N = 0 := Nat.div_eq_of_lt hlt
      omega
  have hdt : d * (q * e + r * m) = x * e + r * N := by
    calc
      d * (q * e + r * m) = (d * q) * e + r * (d * m) := by
        simp [Nat.mul_add, Nat.mul_assoc, Nat.mul_left_comm]
      _ = (d * q) * e + r * (N + e) := by rw [hdm_eq]
      _ = (d * q + r) * e + r * N := by
        simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc]
        omega
      _ = x * e + r * N := by rw [hx]
  have hrd : r ≤ d := by
    change x % d ≤ d
    exact Nat.le_of_lt (Nat.mod_lt x hd)
  have hdN : d * N = (d - r) * N + r * N := by
    calc
      d * N = ((d - r) + r) * N := by rw [Nat.sub_add_cancel hrd]
      _ = (d - r) * N + r * N := by rw [Nat.add_mul]
  have hcond : x * e < (d - r) * N ↔ q * e + r * m < N := by
    constructor
    · intro h
      apply (Nat.mul_lt_mul_left hd).mp
      rw [hdt, hdN]
      omega
    · intro h
      have hh : d * (q * e + r * m) < d * N := (Nat.mul_lt_mul_left hd).mpr h
      rw [hdt, hdN] at hh
      omega
  change exactAt x d s ↔ x * excess d s < (d - x % d) * 2 ^ s
  change exactAt x d s ↔ x * e < (d - r) * N
  rw [hexact_iff, hcond]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_ResidueSensitiveNoCorrection
