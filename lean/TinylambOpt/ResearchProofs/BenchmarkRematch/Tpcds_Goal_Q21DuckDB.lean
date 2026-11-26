import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q21DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q21DuckDB.Goal := by
  intro rows cut
  induction rows with
  | nil => simp [total]
  | cons r rows ih =>
      rcases r with ⟨d, q⟩
      simp [total] at ih ⊢
      constructor
      · by_cases h : d < cut
        · simp [h, ih.1]
        · simp [h, ih.1]
      · by_cases h : d < cut
        · simp [h, ih.2]
        · have hge : cut ≤ d := by omega
          simp [h, hge, ih.2]
