import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Product.CombineFrames` — An admitted merge reconstructs the final scalar without
  retaining input rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_CombineFrames

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Product.CombineFrames.Goal := by
  apply combineFrames_of
  intro a b ha hb
  have h : (a ++ b).prod = a.prod * b.prod := List.prod_append
  simpa [Product.model, List.prod_eq_foldl] using h.symm

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_CombineFrames
