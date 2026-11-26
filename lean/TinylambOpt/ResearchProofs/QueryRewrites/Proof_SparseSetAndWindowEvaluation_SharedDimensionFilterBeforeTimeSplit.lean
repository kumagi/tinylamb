import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_SharedDimensionFilterBeforeTimeSplit

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# SharedDimensionFilterBeforeTimeSplit

Applying the shared eligible-row predicate before splitting the two time windows
preserves both counts, including repeated sales. The executor can materialize one
filtered row list and reuse it for every window probe.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.QueryRewrites (countWhere)

/-! Counting a key-only predicate after projecting eligible rows is the same as
counting it on the original rows with the eligibility flag conjoined. -/
theorem countWhere_filter_map_fst (facts : List (Nat × Bool)) (p : Nat → Bool) :
    countWhere ((facts.filter Prod.snd).map Prod.fst) p =
      countWhere facts (fun r => r.2 && p r.1) := by
  induction facts with
  | nil => rfl
  | cons r facts ih =>
      rcases r with ⟨k, ok⟩
      by_cases hok : ok = true <;> by_cases hk : p k = true <;>
        simpa [countWhere, hok, hk] using ih

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.SharedDimensionFilterBeforeTimeSplit.Goal := by
  intro facts hour
  dsimp only
  simp only [List.map_cons, List.map_nil, List.cons.injEq, and_true]
  constructor
  · exact countWhere_filter_map_fst facts (fun k => decide (8 ≤ hour k ∧ hour k ≤ 8 + 1))
  · exact countWhere_filter_map_fst facts (fun k => decide (19 ≤ hour k ∧ hour k ≤ 19 + 1))
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_SharedDimensionFilterBeforeTimeSplit
