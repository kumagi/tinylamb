import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `BitUnion.CombineFrames` — An admitted merge reconstructs the final scalar without
  retaining input rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_CombineFrames

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitUnion.CombineFrames.Goal := by
  apply combineFrames_of
  intro a b ha hb
  have h := foldl_or_append a b
  simpa [BitUnion.model] using h.symm

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_CombineFrames
