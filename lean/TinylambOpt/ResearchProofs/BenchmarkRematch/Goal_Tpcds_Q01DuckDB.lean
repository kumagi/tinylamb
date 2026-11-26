import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q01DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- If `q` implies `p` pointwise on `xs`, pre-filtering by `p` does not change
the `q`-filter. -/
private theorem filter_subset {α : Type} (p q : α → Bool) :
    ∀ (xs : List α), (∀ r ∈ xs, q r → p r) → (xs.filter p).filter q = xs.filter q
  | [], _ => rfl
  | a :: l, h => by
    by_cases hq : q a
    · have hp : p a := h a (List.Mem.head l) hq
      simp [hq, hp, filter_subset p q l (fun r rl => h r (List.Mem.tail a rl))]
    · by_cases hp : p a
      · simp [hq, hp, filter_subset p q l (fun r rl => h r (List.Mem.tail a rl))]
      · simp [hq, hp, filter_subset p q l (fun r rl => h r (List.Mem.tail a rl))]

theorem sound : Q01DuckDB.Goal := by
  intro returns tn store htn
  refine filter_subset (fun r => tn r.1) (fun r => r.1 == store) returns ?_
  intro r _ hq
  have : r.1 = store := by simpa using hq
  subst this
  exact htn

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q01DuckDB
