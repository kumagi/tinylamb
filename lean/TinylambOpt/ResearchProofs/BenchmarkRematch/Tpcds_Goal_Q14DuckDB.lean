import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q14DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem members_contains {α : Type} [BEq α] [LawfulBEq α] (a b : List α) (k : α) :
    (members a b).contains k = (a.contains k && b.contains k) := by
  simp [members, List.mem_eraseDups]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q14DuckDB.Goal := by
  intro a b c k
  rw [members_contains, members_contains]
