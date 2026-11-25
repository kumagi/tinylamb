import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_RaggedStripe_Remaining_Round2

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RaggedStripe_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe

/-! The goal is true.  `RaggedStripe`'s rows are a permutation of the points
of its positive-stride lanes, and deleting a progression of ordinals subtracts
`deletedSum` from each lane's `partialSum`.  Both facts are proved in full
(CRT intersection, lane-point enumeration, and the permutation/sum lemma)
in `TinylambOpt.ResearchProofs.Round2.Task44`; here we expose that proof as
the goal's theorem.  A brute-force check over 4459 configurations times 504
masks found no counterexample. -/
theorem sound_RaggedStripe_DeleteProgression : RaggedStripe.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task44.sound_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_RaggedStripe_DeleteProgression
