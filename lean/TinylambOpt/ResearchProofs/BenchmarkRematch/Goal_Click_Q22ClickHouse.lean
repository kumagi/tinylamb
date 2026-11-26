import TinylambOpt.ResearchGoals.BenchmarkRematch.Click
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
import TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
import TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group2

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q22ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
open TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
open TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group2

theorem sound : Q22ClickHouse.Goal := by
  intro rows phrase _
  have := freq_filter_sum (fun (r : String × String) => r.1.contains "google" && r.2 == phrase) rows
  unfold countWhere
  have e : (rows.filter (fun r => r.1.contains "google" && r.2 == phrase)) =
      (rows.filter (fun (u,p) => p == phrase && u.contains "google")) := by
    apply List.filter_congr
    intro x _
    obtain ⟨u, p⟩ := x
    exact Bool.and_comm _ _
  rw [← e]
  exact this

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q22ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q22ClickHouse.sound
