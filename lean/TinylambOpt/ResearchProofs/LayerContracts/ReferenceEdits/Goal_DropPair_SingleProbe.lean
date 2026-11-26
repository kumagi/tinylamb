import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_SingleProbe

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : DropPair.SingleProbe.Goal := by
  intro s a b hs
  have hi := ok_iff_not_bad_single DropPair.model frameKeys_DropPair (fun _ _ => rfl) s a b hs
  exact singleProbe_of_head_iff DropPair.model (fun _ _ => rfl) s a b hi.symm

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_SingleProbe
