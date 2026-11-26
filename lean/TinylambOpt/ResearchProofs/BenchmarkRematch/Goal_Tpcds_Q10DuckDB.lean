import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q10DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem memMembers {α : Type} [BEq α] [LawfulBEq α] (a b : List α) (k : α) :
    k ∈ members a b ↔ (k ∈ a ∧ k ∈ b) := by
  simp [members, List.mem_filter, List.mem_eraseDups]

theorem sound : Q10DuckDB.Goal := by
  intro customers s w c
  refine List.filter_congr ?_
  intro k _
  apply Bool.eq_iff_iff.mpr
  simp only [List.contains_iff_mem, Bool.and_eq_true, Bool.or_eq_true, memMembers, List.mem_append]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q10DuckDB
