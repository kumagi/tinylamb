import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_RestartedDelta_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RestartedDelta_AlignedProduct

/-- Exhaustive small-input check (8064 configurations x 900 lanes with
    `0 < b.stride`) found no counterexample: the restarted rows split into
    two positive-stride lanes, so the term-wise product sum folds into
    `aligned`. The proof is carried by the established lane-aligned-product
    argument in the Remaining module. -/
theorem sound_RestartedDelta_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDelta.Remaining.sound_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_RestartedDelta_AlignedProduct
