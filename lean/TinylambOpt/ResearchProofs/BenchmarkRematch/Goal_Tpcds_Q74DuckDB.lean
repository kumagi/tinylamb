import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q74DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- Both paths must have supplied both years: membership in the intersection
of the two member sets is the conjunction of the four probes. -/
theorem sound : Q74DuckDB.Goal := by
  intro s1 s2 w1 w2 k
  classical
  rw [Bool.eq_iff_iff]
  simp only [members, List.contains_eq_mem, List.mem_filter, List.mem_eraseDups,
    decide_eq_true_eq, Bool.and_eq_true]
  simp only [and_assoc]

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q74DuckDB.sound
