import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_RewriteMinimumAlsoMinimizesPeak

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Refutation of `RewriteMinimumAlsoMinimizesPeak`: the write-minimising
    rotation of a three-run tree can still have a larger temporary-file peak. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.RewriteMinimumAlsoMinimizesPeak.Goal := by
  intro h
  -- a = source 5, b = source 0, c = merge(source 3, source 2):
  -- both rotations write 20 bytes, but peaks are 20 and 15 respectively.
  have H := h (Run.source 5) (Run.source 0)
    (Run.merge (Run.source 3) (Run.source 2)) (by native_decide)
  have hnot : ¬ peak (Run.merge (Run.merge (Run.source 5) (Run.source 0))
      (Run.merge (Run.source 3) (Run.source 2))) ≤
      peak (Run.merge (Run.source 5)
        (Run.merge (Run.source 0) (Run.merge (Run.source 3) (Run.source 2)))) := by
    native_decide
  exact hnot H
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_RewriteMinimumAlsoMinimizesPeak
