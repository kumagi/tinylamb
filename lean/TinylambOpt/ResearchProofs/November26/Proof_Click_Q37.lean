import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q37
open TinylambOpt.ResearchGoals.November26.Click
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem takeWhile_length_le (xs : List Nat) (hsorted : xs.Pairwise (· ≤ ·))
    (lo hi : Nat) (hlohi : lo ≤ hi) :
    (xs.takeWhile (fun x => x < lo)).length ≤ (xs.takeWhile (fun x => x ≤ hi)).length := by
  induction xs with
  | nil => simp
  | cons d ds ih =>
      have hp := List.pairwise_cons.mp hsorted
      by_cases hdlo : d < lo
      · have hdhi : d ≤ hi := Nat.le_trans (Nat.le_of_lt hdlo) hlohi
        simp [hdlo, hdhi]
        exact ih hp.2
      · by_cases hdhi : d ≤ hi
        · simp [hdlo, hdhi]
        · simp [hdlo, hdhi]

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q37.Goal := by
  intro days lo hi hpair hlohi
  induction days with
  | nil => simp [countWhere]
  | cons d ds ih =>
      have hp := List.pairwise_cons.mp hpair
      have hds : ds.Pairwise (fun a b => a ≤ b) := hp.2
      by_cases hdlo : d < lo
      · have hdhi : d ≤ hi := Nat.le_trans (Nat.le_of_lt hdlo) hlohi
        simp [hdlo, hdhi, countWhere]
        exact ih hds
      · have hdlo_le : lo ≤ d := Nat.le_of_not_gt hdlo
        by_cases hdhi : d ≤ hi
        · have hcount : countWhere (d :: ds) (fun x => lo ≤ x && x ≤ hi) =
              countWhere ds (fun x => lo ≤ x && x ≤ hi) + 1 := by
            unfold countWhere
            simp [hdlo_le, hdhi]
          rw [hcount]
          simp [hdlo, hdhi]
          have hnone_lo : ∀ x ∈ ds, ¬ x < lo := by
            intro x hx
            exact Nat.not_lt_of_ge (Nat.le_trans hdlo_le (hp.1 x hx))
          have htwlo : ∀ ys : List Nat, (∀ x ∈ ys, ¬ x < lo) → ys.takeWhile (fun x => x < lo) = [] := by
            intro ys
            induction ys with
            | nil => simp
            | cons x xs ih =>
                intro h
                simp [h x (by simp), ih (by intro y hy; exact h y (by simp [hy]))]
          have hloempty := htwlo ds hnone_lo
          have hle := takeWhile_length_le ds hds lo hi hlohi
          have hi_eq := ih hds
          simp [hloempty] at hle hi_eq ⊢
          omega
        · have hhid : hi < d := Nat.lt_of_not_ge hdhi
          have hnone : ∀ x ∈ ds, hi < x := by
            intro x hx
            exact Nat.lt_of_lt_of_le hhid (hp.1 x hx)
          have hzero : countWhere ds (fun x => lo ≤ x && x ≤ hi) = 0 := by
            unfold countWhere
            rw [← List.countP_eq_length_filter]
            exact List.countP_eq_zero.mpr (by
              intro x hx
              simp [Nat.not_le_of_gt (hnone x hx)])
          simpa [hdlo, hdhi, countWhere] using hzero.symm
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q37
