import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `ForwardCopy.SafeDirection` — `∀ j n, j.dst≤j.src → rawFree kernel j (List.range n)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ForwardCopy_SafeDirection

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.ForwardCopy
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : ForwardCopy.SafeDirection.Goal := by
  exact fwd_safe

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ForwardCopy_SafeDirection
