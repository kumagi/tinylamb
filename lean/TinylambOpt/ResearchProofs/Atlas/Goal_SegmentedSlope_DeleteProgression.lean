import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.OrdinalCounterAndSlopeProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `SegmentedSlope.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SegmentedSlope_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope

/-- The two stride-one lanes of `SegmentedSlope` cover exactly the physical
rows of the layout (up to `split` and from `split` onwards). The generic
lane deletion lemma shows that removing a progression of ordinals subtracts
`deletedSum` from each lane's `partialSum`, which is precisely this goal.
The reusable lemma chain is established in `Auto_EncodedPages_E1`. -/
theorem sound_SegmentedSlope_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.OrdinalCounterAndSlopeProgressions.sound_SegmentedSlope_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_SegmentedSlope_DeleteProgression
