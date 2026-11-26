import TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

/-!
Partitioning for a spillable SQL operator must satisfy the modeled size or resource
bound for its partitions.

Proved

- `GeometricProgress` — A non-stagnant oversized path of d passes has a geometric bound
  on its bytes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_GeometricProgress

open TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

theorem sound : GeometricProgress.Goal := by
  intro path n hcond
  induction path generalizing n with
  | nil => simp
  | cons p rest ih =>
      have hall := List.all_eq_true.mp hcond
      have hfirstBool : (!(stagnant p n)) = true := hall (n,p) (by simp)
      have hineq : 5 * p ≤ 4 * n := by
        simpa [stagnant] using hfirstBool
      have hrest : ((p :: rest).zip rest).all (fun x : Nat × Nat => !(stagnant x.2 x.1)) = true := by
        apply List.all_eq_true.mpr
        intro x hx
        exact hall x (List.mem_cons_of_mem _ hx)
      have hi := ih p hrest
      -- hi : 5 ^ rest.length * rest.getLast?.getD p ≤ 4 ^ rest.length * p
      have hmul1 := Nat.mul_le_mul_left 5 hi
      have hmul2 := Nat.mul_le_mul_left (4 ^ rest.length) hineq
      have hlast : (p :: rest).getLast?.getD n = rest.getLast?.getD p := by
        cases hopt : (p :: rest).getLast? with
        | none =>
            exact False.elim (by
              have hne : (p :: rest).getLast? ≠ none := by
                intro hn
                have hempty : p :: rest = [] := List.getLast?_eq_none_iff.mp hn
                simp at hempty
              exact hne hopt)
        | some l =>
            cases rest with
            | nil =>
                simp at hopt
                subst l
                simp
            | cons x xs =>
                have hlastrest : (x :: xs).getLast? = some l := by
                  simpa [List.getLast?_cons_cons] using hopt
                simp [hlastrest]
      rw [hlast]
      simp [Nat.pow_succ, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm] at hmul1 hmul2 ⊢
      omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_GeometricProgress
