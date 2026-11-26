import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-!
Adding one bit of reciprocal precision cannot turn an exact quotient into an
overestimate: the rounded-up multiplier at the finer precision is at most twice
the old one, and its error is never larger than twice the old error.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_RadixPrecisionMonotonicity

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem sound : RadixPrecisionMonotonicity.Goal := by
  intro x d s hd hexact
  have char (d s z : Nat) (hdp : 0 < d) :
      exactAt z d s ↔ z * excess d s < (d - z % d) * 2 ^ s := by
    let N := 2 ^ s
    let m := magic d s
    let e := excess d s
    let q := z / d
    let r := z % d
    have hN : 0 < N := by
      dsimp [N]
      exact Nat.two_pow_pos s
    have hm : m = (N + d - 1) / d := by simp [m, magic, N]
    have hN_le : N ≤ d * m := by
      have hdiv := Nat.div_add_mod (N + d - 1) d
      have hlt := Nat.mod_lt (N + d - 1) hdp
      rw [← hm] at hdiv
      omega
    have he : e = d * m - N := by simp [e, excess, m, N]
    have hdm_eq : d * m = N + e := by omega
    have hz : d * q + r = z := by simp [q, r, Nat.div_add_mod]
    have hzm : z * m = q * N + (q * e + r * m) := by
      rw [← hz]
      calc
        (d * q + r) * m = q * (d * m) + r * m := by
          simp [Nat.add_mul, Nat.mul_assoc, Nat.mul_left_comm]
        _ = q * (N + e) + r * m := by rw [hdm_eq]
        _ = q * N + (q * e + r * m) := by
          simp [Nat.mul_add]
          omega
    have hguess : guess z d s = q + (q * e + r * m) / N := by
      unfold guess
      change (z * m) / N = q + (q * e + r * m) / N
      rw [hzm]
      rw [Nat.mul_comm q N, Nat.add_comm]
      rw [Nat.add_mul_div_left _ _ hN]
      omega
    have hexact_iff : exactAt z d s ↔ q * e + r * m < N := by
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
        have hzdiv : (q * e + r * m) / N = 0 := Nat.div_eq_of_lt hlt
        omega
    have hdt : d * (q * e + r * m) = z * e + r * N := by
      calc
        d * (q * e + r * m) = (d * q) * e + r * (d * m) := by
          simp [Nat.mul_add, Nat.mul_assoc, Nat.mul_left_comm]
        _ = (d * q) * e + r * (N + e) := by rw [hdm_eq]
        _ = (d * q + r) * e + r * N := by
          simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc]
          omega
        _ = z * e + r * N := by rw [hz]
    have hrd : r ≤ d := by
      change z % d ≤ d
      exact Nat.le_of_lt (Nat.mod_lt z hdp)
    have hdN : d * N = (d - r) * N + r * N := by
      calc
        d * N = ((d - r) + r) * N := by rw [Nat.sub_add_cancel hrd]
        _ = (d - r) * N + r * N := by rw [Nat.add_mul]
    have hcond : z * e < (d - r) * N ↔ q * e + r * m < N := by
      constructor
      · intro h
        apply (Nat.mul_lt_mul_left hdp).mp
        rw [hdt, hdN]
        omega
      · intro h
        have hh : d * (q * e + r * m) < d * N := (Nat.mul_lt_mul_left hdp).mpr h
        rw [hdt, hdN] at hh
        omega
    change exactAt z d s ↔ z * excess d s < (d - z % d) * 2 ^ s
    change exactAt z d s ↔ z * e < (d - r) * N
    rw [hexact_iff, hcond]
  let N := 2 ^ s
  let m := magic d s
  let m' := magic d (s + 1)
  let e := excess d s
  let e' := excess d (s + 1)
  have hN : 0 < N := by
    dsimp [N]
    exact Nat.two_pow_pos s
  have hNm : m = (N + d - 1) / d := by simp [m, magic, N]
  have hN_le : N ≤ d * m := by
    have hdiv := Nat.div_add_mod (N + d - 1) d
    have hlt := Nat.mod_lt (N + d - 1) hd
    rw [← hNm] at hdiv
    omega
  have he : e = d * m - N := by simp [e, excess, m, N]
  have hdm_eq : d * m = N + e := by omega
  have hN2 : 2 ^ (s + 1) = 2 * N := by
    dsimp [N]
    rw [Nat.pow_succ]
    omega
  have hm' : m' = (2 * N + d - 1) / d := by simp [m', magic, hN2]
  have htwodm0 : (2 * m) * d = 2 * (d * m) := by
    simp [Nat.mul_comm, Nat.mul_left_comm]
  have h2N_le : 2 * N ≤ 2 * (d * m) := Nat.mul_le_mul_left 2 hN_le
  have hmle : m' ≤ 2 * m := by
    rw [hm']
    apply (Nat.div_le_iff_le_mul hd).mpr
    omega
  have hN2_le : 2 * N ≤ d * m' := by
    have hdiv := Nat.div_add_mod (2 * N + d - 1) d
    have hlt := Nat.mod_lt (2 * N + d - 1) hd
    rw [← hm'] at hdiv
    omega
  have he' : e' = d * m' - 2 * N := by
    dsimp [e', excess]
    change d * magic d (s + 1) - 2 ^ (s + 1) = d * m' - 2 * N
    rw [hN2]
  have hdmp_le : d * m' ≤ d * (2 * m) := Nat.mul_le_mul_left d hmle
  have htwodm : d * (2 * m) = 2 * (d * m) := by
    simp [Nat.mul_assoc, Nat.mul_left_comm]
  have he_le : e' ≤ 2 * e := by
    rw [he']
    have hsub : d * m' - 2 * N ≤ d * (2 * m) - 2 * N :=
      Nat.sub_le_sub_right hdmp_le (2 * N)
    omega
  have hcond : x * excess d s < (d - x % d) * 2 ^ s :=
    (char d s x hd).mp hexact
  have hcond' : x * e' < (d - x % d) * (2 * N) := by
    change x * e < (d - x % d) * N at hcond
    have hmul : x * e' ≤ x * (2 * e) := Nat.mul_le_mul_left x he_le
    have htwo : x * (2 * e) = 2 * (x * e) := by
      simp [Nat.mul_comm, Nat.mul_left_comm]
    have hle2 : x * e' ≤ 2 * (x * e) := by omega
    have hres2 : (d - x % d) * (2 * N) = 2 * ((d - x % d) * N) := by
      simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    omega
  apply (char d (s + 1) x hd).mpr
  change x * e' < (d - x % d) * 2 ^ (s + 1)
  rw [hN2]
  exact hcond'

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_RadixPrecisionMonotonicity
