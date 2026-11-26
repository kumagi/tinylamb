import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `UnsignedSub.SelfZero` — `∀ x, ir op (x,x)=some 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_SelfZero

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : UnsignedSub.SelfZero.Goal := by
  intro x
  simp [ir, UnsignedSub.op]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_SelfZero
