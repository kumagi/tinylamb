import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q39DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

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

theorem sound : Q39DuckDB.Goal := by
  intro xs m hm
  refine filter_subset (fun r => r.1 == 1 || r.1 == 2) (fun r => r.1 == m) xs ?_
  intro r _ hq
  have hr : r.1 = m := by simpa using hq
  rcases hm with rfl | rfl <;> simp [hr]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q39DuckDB
