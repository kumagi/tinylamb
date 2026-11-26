import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# PositivePriorYearDenominatorFilter

Filtering sales using the prior-year activity predicate leaves the key's sales bucket
intact whenever its prior-year total is positive.

The executor can build second-year aggregate state only for customers eligible for the
ratio calculation.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.PositivePriorYearDenominatorFilter
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.PositivePriorYearDenominatorFilter.Goal := by
  intro prev sales k hk hact
  unfold forKey
  rw [List.filter_filter]
  apply congrArg (fun l : List (Nat × Option Int) => l.map Prod.snd)
  apply List.filter_congr
  intro r hr
  by_cases hkey : r.1 == k
  · have hrk : r.1 = k := by simpa only [beq_iff_eq] using hkey
    subst hrk
    simp [hact]
  · simp [hkey]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.PositivePriorYearDenominatorFilter
