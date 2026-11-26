import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `ReverseInput.TailExact` — A masked partial vector only needs the active lanes to be
  addressable.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_TailExact

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.ReverseInput
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : ReverseInput.TailExact.Goal := by
  exact tailExact_generic ReverseInput.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_TailExact
