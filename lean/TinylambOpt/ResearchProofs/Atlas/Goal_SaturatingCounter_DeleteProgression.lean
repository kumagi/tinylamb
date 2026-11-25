import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E1

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SaturatingCounter_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter

/-- The goal reduces to the generic stride-one lane deletion lemma: the two
SaturatingCounter lanes cover exactly the physical rows, and deleting a
progression of ordinals subtracts `deletedSum` from each lane's `partialSum`.
That lemma chain is already established for this model. -/
theorem sound_SaturatingCounter_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E1.sound_SaturatingCounter_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_SaturatingCounter_DeleteProgression
