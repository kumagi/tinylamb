import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LaneMajor_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor

/-- Brute-force check over 82,944 small configurations (`n ≤ 3`, `parameter ≤ 2`,
    `0 < b.stride`, values in `[-2,1]`) found no counterexample. The proof
    reuses the established generic row/lane aligned-product lemma instantiated
    for `LaneMajor.layout` in `Auto_EncodedPages_E4`. -/
theorem sound_LaneMajor_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E4.LaneMajor.LaneMajor_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_LaneMajor_AlignedProduct
