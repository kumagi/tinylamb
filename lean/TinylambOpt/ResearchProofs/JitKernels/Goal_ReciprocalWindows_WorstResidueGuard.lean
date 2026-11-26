import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-!
A conservative guard based only on the range maximum is sufficient: if the
reciprocal error times the largest dividend stays below one power-of-two unit,
every smaller dividend is exact regardless of its residue. This lets a SQL
division kernel hoist one guard out of a vector loop.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_WorstResidueGuard

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem sound : WorstResidueGuard.Goal := by
  intro bound d s hd hguard x hx
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
  have hxdiv : d * q + r = x := by simp [q, r, Nat.div_add_mod]
  have hxm : x * m = q * N + (q * e + r * m) := by
    rw [← hxdiv]
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
      _ = x * e + r * N := by rw [hxdiv]
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
  have hxe : x * e < N := by
    have hle : x * e ≤ bound * e := Nat.mul_le_mul_right e hx
    change bound * excess d s < 2 ^ s at hguard
    change bound * e < N at hguard
    omega
  have hres : N ≤ (d - r) * N := by
    have hrpos : 1 ≤ d - r := by
      have hrlt : x % d < d := Nat.mod_lt x hd
      change r < d at hrlt
      omega
    simpa using Nat.mul_le_mul_right N hrpos
  apply hexact_iff.mpr
  apply hcond.mp
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_WorstResidueGuard
