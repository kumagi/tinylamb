import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.RaggedStripeProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `RaggedStripe.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RaggedStripe_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe

/-! The goal is true.  `RaggedStripe`'s rows are a permutation of the points
of its positive-stride lanes, and deleting a progression of ordinals subtracts
`deletedSum` from each lane's `partialSum`.  Both facts are proved in full
(CRT intersection, lane-point enumeration, and the permutation/sum lemma)
in `TinylambOpt.ResearchProofs.Atlas.EncodedPages.RaggedStripeProgressions`; here we expose that proof as
the goal's theorem.  A brute-force check over 4459 configurations times 504
masks found no counterexample. -/
theorem sound_RaggedStripe_DeleteProgression : RaggedStripe.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.RaggedStripeProgressions.sound_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_RaggedStripe_DeleteProgression
