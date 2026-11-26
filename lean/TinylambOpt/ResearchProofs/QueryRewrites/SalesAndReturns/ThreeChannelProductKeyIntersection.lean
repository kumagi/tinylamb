import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# ThreeChannelProductKeyIntersection

Intersecting the channel key sets once preserves the shared brand, class, and category
membership predicate. The executor can reuse that intersection for item filtering.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ThreeChannelProductKeyIntersection
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem members_contains {α : Type} [BEq α] [LawfulBEq α] (a b : List α) (k : α) :
    (members a b).contains k = (a.contains k && b.contains k) := by
  simp [members, List.mem_eraseDups]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ThreeChannelProductKeyIntersection.Goal := by
  intro a b c k
  rw [members_contains, members_contains]
