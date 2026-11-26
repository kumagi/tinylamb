import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `ReverseInput.UnorderedScatter` — Reordering independent stores in a snapshot vector
  preserves the committed buffer.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_UnorderedScatter

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.ReverseInput
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : ReverseInput.UnorderedScatter.Goal := by
  exact unorderedScatter_generic ReverseInput.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ReverseInput_UnorderedScatter
