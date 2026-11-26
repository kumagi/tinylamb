import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# EligibleStoreCustomerReturnTotals

Restricting customer return totals to eligible Tennessee stores preserves the inputs to
the modeled store average. The executor can filter store keys before accumulating the
correlation state.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.EligibleStoreCustomerReturnTotals
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.EligibleStoreCustomerReturnTotals.Goal := by
  intro returns tn store htn
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  by_cases h : r.1 == store
  · have heq : r.1 = store := beq_iff_eq.mp h
    rw [heq]
    simp [htn]
  · simp [h]
