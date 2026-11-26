import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Fixed-residue sum correction: when every lane of a vector shares the same remainder `r`
modulo `d`, the per-lane quotient sum equals one wide division of `sum(xs) - |xs| * r`
by `d`. The aggregate can therefore issue a single division with a length-times-remainder
correction instead of a division per row, keeping SQL SUM(x / d) results unchanged.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_FixedResidueSumCorrection
open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

private theorem sum_decomp (d r : Nat) : ∀ xs : List Nat, (∀ x ∈ xs, x % d = r) →
    xs.sum = d * (xs.map (· / d)).sum + xs.length * r := by
  intro xs
  induction xs with
  | nil => intro _; simp
  | cons a as ih =>
    intro h
    have ha : a % d = r := h a (List.mem_cons_self : a ∈ a :: as)
    have has : ∀ x ∈ as, x % d = r := fun x hx => h x (List.mem_cons_of_mem a hx)
    have hdiv : a = d * (a / d) + r :=
      (Nat.div_add_mod a d).symm.trans (congrArg (d * (a / d) + ·) ha)
    have hsum := ih has
    simp only [List.sum_cons, List.map_cons, List.length_cons]
    rw [hsum, Nat.mul_add, Nat.add_mul, Nat.one_mul]
    omega

theorem sound : FixedResidueSumCorrection.Goal := by
  intro xs d r hd h
  have hde := sum_decomp d r xs h
  have hsub : xs.sum - xs.length * r = d * (xs.map (· / d)).sum := by omega
  rw [hsub, Nat.mul_div_cancel_left _ hd]

end TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_FixedResidueSumCorrection
