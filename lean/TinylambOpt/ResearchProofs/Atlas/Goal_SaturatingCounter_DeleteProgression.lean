import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.OrdinalCounterAndSlopeProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `SaturatingCounter.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
-/

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
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.OrdinalCounterAndSlopeProgressions.sound_SaturatingCounter_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_SaturatingCounter_DeleteProgression
