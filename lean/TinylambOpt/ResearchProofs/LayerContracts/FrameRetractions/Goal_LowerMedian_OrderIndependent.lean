import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_LowerMedian

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `LowerMedian.OrderIndependent` — Changing input order cannot change the output or the
  choice to rescan.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_OrderIndependent

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.CommonLowerMedian
open List

theorem sound : LowerMedian.OrderIndependent.Goal := by
  apply orderIndependent_of
  intro a b p
  show lowerMedian a = lowerMedian b
  exact lowerMedian_of_perm p

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_OrderIndependent
