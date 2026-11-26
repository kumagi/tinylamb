import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# SalesKeyFilterOnReturnJoin

Restricting returns to keys present in sales preserves the modeled three-way join count,
including sales and catalog multiplicities.

The executor can reduce the returns sent to the catalog join using a semi-join on
customer, item, and ticket.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SalesKeyFilterOnReturnJoin
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.SalesKeyFilterOnReturnJoin.Goal := by
  intro sales returns catalog
  dsimp only
  induction returns with
  | nil => rfl
  | cons r returns ih =>
      by_cases hmem : sales.eraseDups.contains r = true
      · have hrmem : r ∈ sales := by
          have herase : r ∈ sales.eraseDups := by simpa using hmem
          exact List.mem_eraseDups.mp herase
        rw [List.filter_cons]
        simp [hrmem, ih]
      · have hnotmem : ¬ r ∈ sales := by
          intro hr
          have herase : r ∈ sales.eraseDups := List.mem_eraseDups.mpr hr
          have hc : sales.eraseDups.contains r = true := by simpa using herase
          exact hmem hc
        have hcnt : sales.count r = 0 := List.count_eq_zero.mpr hnotmem
        rw [List.filter_cons]
        simp [hnotmem, hcnt, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SalesKeyFilterOnReturnJoin
