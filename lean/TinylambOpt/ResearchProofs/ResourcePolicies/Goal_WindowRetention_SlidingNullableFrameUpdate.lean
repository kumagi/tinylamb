import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

/-!
`SlidingNullableFrameUpdate`: incremental sliding SUM over nullable payloads must
track both the exact non-NULL sum and the non-NULL count, keeping "all NULL" distinct
from "sum zero". SQL may update a frame in O(1) instead of rescanning it.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_SlidingNullableFrameUpdate

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : SlidingNullableFrameUpdate.Goal := by
  intro expired entered middle
  generalize hys : middle.filterMap id = ys
  cases expired <;> cases entered <;> cases ys <;> simp [nsum, hys] <;> omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_SlidingNullableFrameUpdate
