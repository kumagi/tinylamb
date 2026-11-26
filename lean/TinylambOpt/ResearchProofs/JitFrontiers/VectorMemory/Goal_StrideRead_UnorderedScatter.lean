import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `StrideRead.UnorderedScatter` — Reordering independent stores in a snapshot vector
  preserves the committed buffer.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_UnorderedScatter

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.StrideRead
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : StrideRead.UnorderedScatter.Goal := by
  exact unorderedScatter_generic StrideRead.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_UnorderedScatter
