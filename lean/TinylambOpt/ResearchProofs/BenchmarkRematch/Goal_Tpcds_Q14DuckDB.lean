import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q14DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem memMembers {α : Type} [BEq α] [LawfulBEq α] (a b : List α) (k : α) :
    k ∈ members a b ↔ (k ∈ a ∧ k ∈ b) := by
  simp [members, List.mem_filter, List.mem_eraseDups]

theorem sound : Q14DuckDB.Goal := by
  intro a b c k
  apply Bool.eq_iff_iff.mpr
  simp only [List.contains_iff_mem, Bool.and_eq_true, memMembers a b, memMembers (members a b) c]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q14DuckDB
