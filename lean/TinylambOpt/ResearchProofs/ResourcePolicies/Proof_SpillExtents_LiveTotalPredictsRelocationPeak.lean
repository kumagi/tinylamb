import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_LiveTotalPredictsRelocationPeak

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch refutation: equal occupied-byte totals do not determine the peak
    temporary space of copy-and-publish compaction, because the largest single
    extent is copied before the old directory can be released. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.LiveTotalPredictsRelocationPeak.Goal := by
  intro h
  have hbad := h [2, 0] [1, 1] (by decide)
  exact (by decide : ¬ ([2, 0].sum + [2, 0].max?.getD 0) =
    ([1, 1].sum + [1, 1].max?.getD 0)) hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_LiveTotalPredictsRelocationPeak
