import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q31ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q31ClickHouse.Goal := by
  intro pairs e ip
  induction pairs with
  | nil => simp
  | cons p ps ih =>
      by_cases h1 : p.1 == e
      · by_cases h2 : p.2 == ip
        · have hp : p = (e, ip) := by
            ext
            · simpa [beq_iff_eq] using h1
            · simpa [beq_iff_eq] using h2
          simp [List.count_cons, h1, h2, hp, ih]
        · have h2f : (p.2 == ip) = false := by simpa using h2
          have hp : p ≠ (e, ip) := by
            intro heq
            exact h2 (by simp [heq])
          simp [List.count_cons, h1, h2f, hp, ih]
      · have h1f : (p.1 == e) = false := by simpa using h1
        have hp : p ≠ (e, ip) := by
          intro heq
          exact h1 (by simp [heq])
        simp [List.count_cons, h1f, hp, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q31ClickHouse
