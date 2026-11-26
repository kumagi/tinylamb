import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `BackwardDelta.ReverseSafe` — `∀ j n, j.src=j.dst → rawFree kernel j (List.range
  n).reverse`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_BackwardDelta_ReverseSafe

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.BackwardDelta
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : BackwardDelta.ReverseSafe.Goal := by
  exact delta_reverse

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_BackwardDelta_ReverseSafe
