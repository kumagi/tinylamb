import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `UnsignedLeftShift.ZeroCount` — `∀ x, ir op (x,0)=some x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedLeftShift_ZeroCount

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : UnsignedLeftShift.ZeroCount.Goal := by
  intro x
  simp [ir, UnsignedLeftShift.op]
  simpa using x.isLt

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedLeftShift_ZeroCount
