import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q08DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch.Q08DuckDB.Goal := by
  intro rows
  induction rows with
  | nil => simp [total]
  | cons r rs ih =>
    rcases r with ⟨p,v⟩
    simp only [total, List.map_cons, List.sum_cons]
    have ih' : (List.map Prod.snd rs).sum = (List.map Prod.snd (List.filter Prod.fst rs)).sum +
        (List.map Prod.snd (List.filter (fun x => !x.fst) rs)).sum := by
      simpa [total] using ih
    by_cases hp : p <;> simp [hp] at * <;> omega

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q08DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q08DuckDB.sound
