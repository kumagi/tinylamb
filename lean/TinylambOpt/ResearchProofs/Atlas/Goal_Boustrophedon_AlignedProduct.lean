import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Boustrophedon_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon

/-- Brute-force check over 5625 configurations and 1800 lanes (all
    `0 < b.stride`, `n ≤ 8`, signed values in a small range) found no
    counterexample.  The proof reuses the established generic row/lane
    aligned-product argument in `Auto_EncodedPages_E4`: the snake-order rows
    permute to the striped lanes, all of which have positive stride, so the
    term-wise product sum folds into `aligned`. -/
theorem sound_Boustrophedon_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4.Boustrophedon.Boustrophedon_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_Boustrophedon_AlignedProduct
