import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Last-residue representative: exactness of the reciprocal guess within one residue class
mod `d` is downward closed along the class (the accumulated error grows with the
dividend). So if the largest reachable dividend of every residue class in a profiled
window is certified exact, every smaller dividend in the window is exact too, and the
kernel can validate one representative per class instead of the whole window. SQL
integer-division results are unchanged.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_LastResidueRepresentative
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

theorem sound : LastResidueRepresentative.Goal := by
  intro bound d s hd H x hx
  let y := (bound - x) / d
  have hmodlt : (bound - x) % d < d := Nat.mod_lt (bound - x) hd
  have hdiv : bound - x = d * y + (bound - x) % d := (Nat.div_add_mod (bound - x) d).symm
  have hxb : x + d * y ≤ bound := by omega
  have hgt : bound < x + d * y + d := by omega
  have hmod : (x + d * y) % d = x % d := Nat.add_mul_mod_self_left x d y
  have hdivx : (x + d * y) / d = x / d + y := Nat.add_mul_div_left x y hd
  have ht := (char (x + d * y) d s hd).mp (H (x + d * y) hxb hgt)
  rw [hmod, hdivx, Nat.add_mul] at ht
  exact (char x d s hd).mpr (by omega)

end TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_LastResidueRepresentative
