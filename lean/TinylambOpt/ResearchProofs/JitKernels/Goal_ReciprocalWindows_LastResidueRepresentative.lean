import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-!
For a fixed residue, exactness of the reciprocal kernel is monotone in the
quotient. Testing the largest profiled dividend in each residue class therefore
certifies every smaller dividend of that class in a SQL division window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_LastResidueRepresentative

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem sound : LastResidueRepresentative.Goal := by
  intro bound d s hd hlast x hx
  have char : ∀ z, exactAt z d s ↔ z * excess d s < (d - z % d) * 2 ^ s := by
    intro z
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
      have hlt := Nat.mod_lt (N + d - 1) hd
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
      exact Nat.le_of_lt (Nat.mod_lt z hd)
    have hdN : d * N = (d - r) * N + r * N := by
      calc
        d * N = ((d - r) + r) * N := by rw [Nat.sub_add_cancel hrd]
        _ = (d - r) * N + r * N := by rw [Nat.add_mul]
    have hcond : z * e < (d - r) * N ↔ q * e + r * m < N := by
      constructor
      · intro h
        apply (Nat.mul_lt_mul_left hd).mp
        rw [hdt, hdN]
        omega
      · intro h
        have hh : d * (q * e + r * m) < d * N := (Nat.mul_lt_mul_left hd).mpr h
        rw [hdt, hdN] at hh
        omega
    change exactAt z d s ↔ z * excess d s < (d - z % d) * 2 ^ s
    change exactAt z d s ↔ z * e < (d - r) * N
    rw [hexact_iff, hcond]
  let k := (bound - x) / d
  let rem := (bound - x) % d
  let y := bound - rem
  have hrem : rem < d := by
    dsimp [rem]
    exact Nat.mod_lt _ hd
  have hrem_le : rem ≤ bound := by
    have h1 : rem ≤ bound - x := by
      dsimp [rem]
      exact Nat.mod_le _ _
    exact Nat.le_trans h1 (Nat.sub_le _ _)
  have hy_le : y ≤ bound := by
    dsimp [y]
    exact Nat.sub_le _ _
  have hy_big : bound < y + d := by
    have hyadd : y + rem = bound := by
      dsimp [y]
      exact Nat.sub_add_cancel hrem_le
    omega
  have hyx : y = x + k * d := by
    have hdiv : d * k + rem = bound - x := by
      dsimp [k, rem]
      exact Nat.div_add_mod (bound - x) d
    have hdiv' : k * d + rem = bound - x := by simpa [Nat.mul_comm] using hdiv
    have hsub : bound - x + x = bound := Nat.sub_add_cancel hx
    have hyadd : y + rem = bound := by
      dsimp [y]
      exact Nat.sub_add_cancel hrem_le
    omega
  have hry : y % d = x % d := by
    rw [hyx, Nat.mul_comm k d, Nat.add_mul_mod_self_left]
  have hycond := (char y).mp (hlast y hy_le hy_big)
  have hxcond : x * excess d s < (d - x % d) * 2 ^ s := by
    rw [hry] at hycond
    have hle : x ≤ y := by rw [hyx]; omega
    have hmul : x * excess d s ≤ y * excess d s := Nat.mul_le_mul_right _ hle
    omega
  exact (char x).mpr hxcond

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_LastResidueRepresentative
