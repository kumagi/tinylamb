import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q43DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q43DuckDB.Goal := by
  intro minutes
  dsimp only
  let page := TinylambOpt.ResearchGoals.BenchmarkRematch.ranges minutes.eraseDups 1000 10
  apply List.map_congr_left
  intro m hm
  have hc : page.contains m = true := by simpa [page] using hm
  rw [List.count_filter hc]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q43DuckDB
