import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `IndexedScatter.UnorderedScatter` — Reordering independent stores in a snapshot
  vector preserves the committed buffer.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedScatter_UnorderedScatter

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.IndexedScatter
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : IndexedScatter.UnorderedScatter.Goal := by
  exact unorderedScatter_generic IndexedScatter.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedScatter_UnorderedScatter
