import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q38ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem aux : ∀ zs : List (Bool × String),
    TinylambOpt.ResearchGoals.BenchmarkRematch.countWhere zs (fun (b, t) => b && t != "") =
      ((zs.filterMap (fun (b, t) => if b then some t else none)).filter (· != "")).length := by
  intro zs
  induction zs with
  | nil => simp [TinylambOpt.ResearchGoals.BenchmarkRematch.countWhere]
  | cons z zs ih =>
      rcases z with ⟨b, t⟩
      by_cases hb : b <;> by_cases ht : t = "" <;>
        (simp [TinylambOpt.ResearchGoals.BenchmarkRematch.countWhere, hb, ht] at ih ⊢; exact ih)

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q38ClickHouse.Goal := by
  intro flags titles
  exact aux (flags.zip titles)

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q38ClickHouse
