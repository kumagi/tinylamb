import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `ExactRightShift.WidthCount` — `∀ x, ir op (x,64)=none`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_ExactRightShift_WidthCount

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : ExactRightShift.WidthCount.Goal := by
  intro x
  simp [ir, ExactRightShift.op]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_ExactRightShift_WidthCount
