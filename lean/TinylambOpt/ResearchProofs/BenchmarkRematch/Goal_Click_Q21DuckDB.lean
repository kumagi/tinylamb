import TinylambOpt.ResearchGoals.BenchmarkRematch.Click
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
import TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
import TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group2

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q21DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
open TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
open TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group2

theorem sound : Q21DuckDB.Goal := by
  intro urls
  exact freq_filter_sum (fun s => s.contains "google") urls

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q21DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q21DuckDB.sound
