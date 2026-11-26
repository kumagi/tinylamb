import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q74DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q74DuckDB.Goal := by
  intro s1 s2 w1 w2 k
  dsimp only [members]
  simp [List.mem_eraseDups, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q74DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q74DuckDB.sound
