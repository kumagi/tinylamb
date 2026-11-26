import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `StrideRead.CompressInPlace` — `∀ j n, j.dst≤j.src → 0<j.stride → rawFree kernel j
  (List.range n)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_CompressInPlace

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.StrideRead
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : StrideRead.CompressInPlace.Goal := by
  exact stride_compress

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideRead_CompressInPlace
