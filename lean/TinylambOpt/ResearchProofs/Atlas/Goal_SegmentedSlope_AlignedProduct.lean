import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P2

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SegmentedSlope_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope

/-- The two stride-one lanes of SegmentedSlope decode exactly the physical rows,
    so the generic aligned-product lemma for row/lane covers applies. The proof
    is supplied by the already checked generic development. -/
theorem sound_SegmentedSlope_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P2.sound_SegmentedSlope_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_SegmentedSlope_AlignedProduct
