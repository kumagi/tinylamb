import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# LateDistinctOnWinningSearchPhrases

For groups reaching the count boundary after both text predicates, delaying
user aggregation leaves their exact distinct sets intact. This allows the
engine to filter to winning phrase groups before aggregating users.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_LateDistinctOnWinningSearchPhrases

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.LateDistinctOnWinningSearchPhrases.Goal := by
  intro rs phrase threshold hpass
  simp only [forKey, List.filter_filter]
  intro hpass_phrase
  congr 1
  congr 1
  apply List.filter_congr
  intro r _
  by_cases hr : r.1 == phrase
  · have hkeq : r.1 = phrase := eq_of_beq hr
    subst hkeq
    simp [hpass_phrase]
  · simp [hr]


end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_LateDistinctOnWinningSearchPhrases
