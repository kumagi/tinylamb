import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# SinglePassTimeBandCount

A single pass can maintain the modeled morning and afternoon counts separately. The
executor can share the fact scan.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SinglePassTimeBandCount
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem morning (h : Nat) : ([8,9,19,20].contains h && decide (h ≤ 9)) = [8,9].contains h := by
  by_cases h8 : h = 8 <;> by_cases h9 : h = 9 <;> by_cases h19 : h = 19 <;> by_cases h20 : h = 20 <;>
    simp [h8, h9, h19, h20] <;> omega

theorem afternoon (h : Nat) : ([8,9,19,20].contains h && decide (19 ≤ h)) = [19,20].contains h := by
  by_cases h8 : h = 8 <;> by_cases h9 : h = 9 <;> by_cases h19 : h = 19 <;> by_cases h20 : h = 20 <;>
    simp [h8, h9, h19, h20] <;> omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.SinglePassTimeBandCount.Goal := by
  intro hours
  dsimp only
  simp only [countWhere, List.filter_filter]
  congr 1
  · apply congrArg List.length
    apply List.filter_congr
    intro h _
    simpa [Bool.and_comm] using morning h
  · apply congrArg List.length
    apply List.filter_congr
    intro h _
    simpa [Bool.and_comm] using afternoon h

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SinglePassTimeBandCount

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SinglePassTimeBandCount.sound
