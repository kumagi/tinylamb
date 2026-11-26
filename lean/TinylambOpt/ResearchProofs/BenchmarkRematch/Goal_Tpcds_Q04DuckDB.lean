import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q04DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem beq_decide (x y : Nat) : (x == y) = decide (x = y) := rfl

theorem sound : Q04DuckDB.Goal := by
  intro xs c y hc hy
  refine List.filter_congr ?_
  intro r _
  cases r with
  | mk k v => cases k with
    | mk a b =>
      have hbe : ((a, b) == (c, y)) = (a == c && b == y) := rfl
      rw [hbe]
      apply Bool.eq_iff_iff.mpr
      rw [beq_decide a c, beq_decide b y, beq_decide (2 * a + b) (2 * c + y)]
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      omega

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q04DuckDB
