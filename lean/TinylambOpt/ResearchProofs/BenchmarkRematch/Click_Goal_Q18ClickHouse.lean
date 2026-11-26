import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q18ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem map_count_filter {α : Type} [BEq α] [LawfulBEq α]
    (xs s : List α) (p : α → Bool) (hp : ∀ a ∈ s, p a = true) :
    s.map (fun k => (xs.filter p).count k) = s.map xs.count := by
  induction s with
  | nil => simp
  | cons a s ih =>
      have hp' : ∀ b ∈ s, p b = true := by
        intro b hb
        exact hp b (by simp [hb])
      have ha : p a = true := hp a (by simp)
      have ih' := ih hp'
      simp [List.count_filter ha, ih']

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q18ClickHouse.Goal := by
  intro xs
  dsimp only
  apply map_count_filter
  intro a ha
  simpa using ha
end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q18ClickHouse
