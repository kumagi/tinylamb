import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `PackedBits.WordBoundary` — `∀ j n, (addresses kernel j (List.range
  n)).length=(n+63)/64`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_WordBoundary

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.PackedBits
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : PackedBits.WordBoundary.Goal := by
  exact wordBoundary

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_WordBoundary
