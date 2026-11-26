import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_LateDistinctOnWinningSearchPhrases

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
A phrase that passes the global frequency threshold keeps every one of its
rows in the shortlist: the predicate depends only on the phrase key, so
deferring the distinct-user aggregation until after shortlisting preserves
the exact user set.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.QueryRewrites (forKey)

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.LateDistinctOnWinningSearchPhrases.Goal := by
  intro rs phrase threshold
  dsimp only
  intro hpass
  unfold forKey
  rw [List.filter_filter]
  have hp :
      (fun r : Nat × Nat =>
        r.1 == phrase && decide (threshold ≤ (rs.map Prod.fst).count r.1)) =
        (fun r : Nat × Nat => r.1 == phrase) := by
    funext r
    by_cases h : r.1 == phrase
    · have heq : r.1 = phrase := beq_iff_eq.mp h
      simp [h, heq, hpass]
    · simp [h]
  rw [hp]
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_LateDistinctOnWinningSearchPhrases
