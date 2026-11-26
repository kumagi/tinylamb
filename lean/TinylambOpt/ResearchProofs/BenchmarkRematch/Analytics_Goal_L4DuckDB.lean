import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_L4DuckDB

theorem sum_replicate_flatten (n : Nat) (xs : List Int) : (List.replicate n xs).flatten.sum = (n : Int) * xs.sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.flatten_cons, List.sum_append, ih]
      rw [Int.natCast_add, Int.add_mul]
      simp [Int.add_comm]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.L4DuckDB.Goal := by
  intro scores forumCopies
  dsimp only
  induction forumCopies with
  | zero => simp
  | succ n ih =>
      have hsum := sum_replicate_flatten (n + 1) (List.filterMap id scores)
      simp [ih, hsum]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_L4DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_L4DuckDB.sound
