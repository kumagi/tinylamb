import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q20ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q20ClickHouse.Goal := by
  intro xs k
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : x == k
      · have hxk : x = k := beq_iff_eq.mp h
        subst hxk
        simp [List.count_cons, List.replicate_succ, ih]
      · simp [List.count_cons, h, ih]
end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q20ClickHouse
