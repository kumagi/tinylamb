import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# RestrictMonthStatisticsBeforeSelfJoin

Only January and February inventory statistics can contribute to the modeled month
comparison. The executor can build states for those months alone.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RestrictMonthStatisticsBeforeSelfJoin

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.RestrictMonthStatisticsBeforeSelfJoin.Goal := by
  intro xs m hm
  rcases hm with rfl | rfl
  all_goals
    rw [List.filter_filter]
    apply List.filter_congr
    intro r hr
    by_cases h1 : (r.1 == 1) = true
    · simp [h1]
    · by_cases h2 : (r.1 == 2) = true
      · simp [h1, h2]
      · simp [h1, h2]

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RestrictMonthStatisticsBeforeSelfJoin

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RestrictMonthStatisticsBeforeSelfJoin.sound
