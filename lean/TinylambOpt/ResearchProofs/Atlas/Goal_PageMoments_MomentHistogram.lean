import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_MomentHistogram

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.PageMoments

/-- The first three page moments do not determine the value histogram:
[-2,1,1] and [-1,-1,2] agree on length, sum, and energy, but not on
the number of entries at least 2. -/
theorem disproof_PageMoments_MomentHistogram : ¬ PageMoments.MomentHistogram.Goal := by
  intro h
  have hres := h ([-2, 1, 1] : Page) ([-1, -1, 2] : Page)
    (by native_decide) (by native_decide) (by native_decide) 2
  simp at hres

end TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_MomentHistogram
