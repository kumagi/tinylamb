import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
import TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

/-! Coalescing already-reduced batches before the final read-modify-write does
not lose contributions: the reduced group for each slot carries exactly its
total signed delta. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_StoreOncePerVectorPair

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
open TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

theorem sound : StoreOncePerVectorPair.Goal := by
  intro a b k
  calc
    delta (combine (combine a ++ combine b)) k = delta (combine a ++ combine b) k := delta_combine _ _
    _ = delta (combine a) k + delta (combine b) k := delta_append _ _ _
    _ = delta a k + delta b k := by rw [delta_combine, delta_combine]
    _ = delta (a ++ b) k := (delta_append a b k).symm

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_StoreOncePerVectorPair
