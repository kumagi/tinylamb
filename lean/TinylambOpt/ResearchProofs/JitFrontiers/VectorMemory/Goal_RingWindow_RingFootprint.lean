import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `RingWindow.RingFootprint` — `∀ j ids, (addresses kernel j ids).length≤j.window+1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_RingFootprint

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.RingWindow
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : RingWindow.RingFootprint.Goal := by
  exact ringFootprint

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_RingFootprint
