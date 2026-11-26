import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q16
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem hit_symm {o r : Option Nat} (h : hit o r = true) : hit r o = true := by
  cases o with
  | none => simp [hit] at h
  | some n =>
      cases r with
      | none => simp [hit] at h
      | some m =>
          simp [hit] at h ⊢
          exact h.symm

theorem any_cons_of_false {α : Type} (p : α → Bool) (a : α) (l : List α) (h : p a = false) :
    (a :: l).any p = l.any p := by
  simp [h]

theorem any_filter_hit (orders returns : List (Option Nat)) (o : Option Nat) (ho : o ∈ orders) :
    (returns.filter (fun r => orders.any (hit r))).any (hit o) = returns.any (hit o) := by
  induction returns with
  | nil => rfl
  | cons r returns ih =>
      by_cases hP : orders.any (hit r) = true
      · have hfilter : (List.filter (fun r => orders.any (hit r)) (r :: returns)) =
            r :: (List.filter (fun r => orders.any (hit r)) returns) := by
          simp [hP]
        rw [hfilter]
        by_cases hr : hit o r = true
        · simp [hr]
        · have hr' : hit o r = false := by simpa using hr
          rw [any_cons_of_false (hit o) r _ hr', any_cons_of_false (hit o) r _ hr']
          exact ih
      · have hP' : orders.any (hit r) = false := by simpa using hP
        have hfilter : (List.filter (fun r => orders.any (hit r)) (r :: returns)) =
            (List.filter (fun r => orders.any (hit r)) returns) := by
          simp [hP']
        rw [hfilter]
        by_cases hr : hit o r = true
        · have : hit r o = true := hit_symm hr
          have hPtrue : orders.any (hit r) = true := List.any_eq_true.mpr ⟨o, ho, this⟩
          rw [hP'] at hPtrue
          contradiction
        · have hr' : hit o r = false := by simpa using hr
          rw [any_cons_of_false (hit o) r _ hr']
          exact ih

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q16.Goal := by
  intro orders returns
  dsimp only
  apply List.filter_congr
  intro o ho
  rw [any_filter_hit orders returns o ho]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q16
