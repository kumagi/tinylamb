import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# StoreAndRemoteCustomerMembership

Combining the store mark with the union of the web and catalog marks preserves the
existence predicate. The executor can construct the qualifying customer set before
probing outer rows.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.StoreAndRemoteCustomerMembership
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem members_contains {α : Type} [BEq α] [LawfulBEq α] (a b : List α) (k : α) :
    (members a b).contains k = (a.contains k && b.contains k) := by
  simp [members, List.mem_eraseDups]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.StoreAndRemoteCustomerMembership.Goal := by
  intro customers s w c
  apply List.filter_congr
  intro k hk
  rw [members_contains]
  simp
