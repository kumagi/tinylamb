import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_J3ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group

theorem sound : J3ClickHouse.Goal := by
  intro cast years
  dsimp only
  refine Prod.ext ?_ ?_
  · have h := group_sum_nat (fun k => (years k).length) cast.length cast (Nat.le_refl _)
    simp only [List.length_flatMap]
    rw [h]
  · have h := group_sum_int (fun k => (years k).sum) cast.length cast (Nat.le_refl _)
    have e : ∀ l : List Nat, (l.flatMap years).sum = (l.map (fun k => (years k).sum)).sum := by
      intro l
      induction l with
      | nil => simp
      | cons k ks ih => simp [List.flatMap_cons, List.sum_append_int, ih]
    rw [e]
    exact h

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_J3ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_J3ClickHouse.sound
