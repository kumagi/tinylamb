import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_DominantChannelRatioTournament

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! The stated goal puts the three positivity hypotheses inside the left side
of the `↔` (because `→` binds tighter than `↔`). For non-positive cardinalities
that left side is vacuously true while the right side can be false, so the
rewrite is not sound as written. The counterexample uses zero numerators and
cardinalities `-1`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem refute : ¬ TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.DominantChannelRatioTournament.Goal := by
  intro h
  have hiff := h 0 0 0 (-1) (-1) (-1)
  have hl : (0 < (-1 : Int) → 0 < (-1 : Int) → 0 < (-1 : Int) →
      (0 * (-1 : Int) > 0 * (-1 : Int) ∧ 0 * (-1 : Int) > 0 * (-1 : Int))) := by
    intro h₁ h₂ h₃
    omega
  have hr := hiff.mp hl
  simp at hr
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_DominantChannelRatioTournament
