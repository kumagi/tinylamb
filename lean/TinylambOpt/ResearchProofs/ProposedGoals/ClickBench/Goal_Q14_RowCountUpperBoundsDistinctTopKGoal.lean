import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q14_RowCountUpperBoundsDistinctTopKGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem eraseDups_length_le {α : Type} [BEq α] (xs : List α) :
    xs.eraseDups.length ≤ xs.length := by
  let motive : Nat → Prop := fun n => ∀ xs : List α, xs.length = n → xs.eraseDups.length ≤ xs.length
  exact (Nat.strongRecOn xs.length (motive := motive) (fun n ih => by
    intro xs hlen
    cases xs with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        simp only [List.length_cons] at hlen
        have h2 := List.length_filter_le (fun b => !b == a) as
        have hsub : (as.filter (fun b => !b == a)).length < n := by omega
        have h1 := ih (as.filter (fun b => !b == a)).length hsub
          (as.filter (fun b => !b == a)) rfl
        simp only [List.length_cons]
        omega)) xs rfl

theorem sound : Q14_RowCountUpperBoundsDistinctTopKGoal := by
  intro users tau h
  exact Nat.lt_of_le_of_lt (eraseDups_length_le users) h

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q14_RowCountUpperBoundsDistinctTopKGoal
