import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_UnmatchedRefundPageFastPath

Registers the proof of the UnmatchedRefundPageFastPath goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_UnmatchedRefundPageFastPath
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.UnmatchedRefundPageFastPath
theorem sound : Goal := by
  intro sales refunds hdisj
  dsimp [Goal, TinylambOpt.ResearchGoals.QueryRewrites.forKey]
  induction sales with
  | nil => simp
  | cons s ss ih =>
      have hk : ¬ s.1 ∈ refunds.map Prod.fst := hdisj s (by simp)
      have hfilter : (refunds.filter (fun r => r.1 == s.1)) = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro r hr
        have hrf := List.mem_filter.mp hr
        have heq : r.1 = s.1 := by simpa using (eq_of_beq hrf.2)
        exact hk (List.mem_map.mpr ⟨r, hrf.1, heq⟩)
      have ih' := ih (fun x hx => hdisj x (by simp [hx]))
      simp only [List.flatMap_cons, hfilter, List.map_nil, List.isEmpty_nil, ite_true,
        List.map_cons, List.singleton_append]
      exact congrArg (List.cons s.2) ih'
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_UnmatchedRefundPageFastPath
