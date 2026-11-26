import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `ForwardCopy.TailExact` — A masked partial vector only needs the active lanes to be
  addressable.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ForwardCopy_TailExact

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.ForwardCopy
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : ForwardCopy.TailExact.Goal := by
  exact tailExact_generic ForwardCopy.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ForwardCopy_TailExact
