import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# MinuteTopKBeforeFrequencyCount

Only minute keys that occur in the data affect the ordered result. The executor can
select the needed ten minute keys before counting their frequencies.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.MinuteTopKBeforeFrequencyCount
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.MinuteTopKBeforeFrequencyCount.Goal := by
  intro minutes
  dsimp only
  let page := TinylambOpt.ResearchGoals.QueryRewrites.ranges minutes.eraseDups 1000 10
  apply List.map_congr_left
  intro m hm
  have hc : page.contains m = true := by simpa [page] using hm
  rw [List.count_filter hc]

end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.MinuteTopKBeforeFrequencyCount
