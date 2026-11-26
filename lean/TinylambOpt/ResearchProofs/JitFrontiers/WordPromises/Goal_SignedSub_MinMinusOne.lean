import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `SignedSub.MinMinusOne` — `ir op (0x8000000000000000,1)=none`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedSub_MinMinusOne

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : SignedSub.MinMinusOne.Goal := by
  change ir SignedSub.op (0x8000000000000000, 1) = none
  native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedSub_MinMinusOne
