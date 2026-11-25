import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P2

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BucketRuns_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns

/-- The BucketRuns lanes split the page into constant-value runs of stride one.
    They cover exactly the physical rows (as a permutation), so the generic
    aligned-product lemma for row/lane covers applies directly. -/
theorem sound_BucketRuns_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P2.BucketRuns.BR_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_BucketRuns_AlignedProduct
