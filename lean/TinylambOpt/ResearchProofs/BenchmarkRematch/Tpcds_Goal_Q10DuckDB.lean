import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q10DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem members_contains {α : Type} [BEq α] [LawfulBEq α] (a b : List α) (k : α) :
    (members a b).contains k = (a.contains k && b.contains k) := by
  simp [members, List.mem_eraseDups]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q10DuckDB.Goal := by
  intro customers s w c
  apply List.filter_congr
  intro k hk
  rw [members_contains]
  simp
