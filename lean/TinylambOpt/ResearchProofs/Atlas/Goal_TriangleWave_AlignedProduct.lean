import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TriangleWave_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave

/-- Exhaustive small-input check (n < 9, parameter < 5, b.n < 6, strides
    1..3, signed values in a small range) found no counterexample. The proof
    reuses the established generic row/lane aligned-product argument: the
    triangle-wave rows permute to the striped lanes, all of which have
    positive stride, so the term-wise product sum folds into `aligned`. -/
theorem sound_TriangleWave_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4.TriangleWave.TriangleWave_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_TriangleWave_AlignedProduct
