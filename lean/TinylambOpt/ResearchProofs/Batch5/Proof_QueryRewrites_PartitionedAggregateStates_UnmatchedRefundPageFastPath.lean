import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_UnmatchedRefundPageFastPath

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
When certified key sets are disjoint, every refund probe for a sale page is
empty. The page's net price is then exactly its original price, so the refund
lookup side stream can be skipped for those rows.
-/

set_option maxHeartbeats 0

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

theorem sound : UnmatchedRefundPageFastPath.Goal := by
  intro sales refunds h
  induction sales with
  | nil => simp
  | cons sale sales ih =>
      rcases sale with ⟨k, p⟩
      have hk : ¬ k ∈ refunds.map Prod.fst := h (k, p) (by simp)
      have hvs : forKey refunds k = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro v hv
        rcases List.mem_map.mp hv with ⟨r, hr, rfl⟩
        have hr' := List.mem_filter.mp hr
        have heq : r.1 = k := beq_iff_eq.mp hr'.2
        apply hk
        apply List.mem_map.mpr
        exact ⟨r, hr'.1, heq⟩
      have ih' := ih (by intro s hs; exact h s (by simp [hs]))
      simpa [hvs] using ih'
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_UnmatchedRefundPageFastPath
