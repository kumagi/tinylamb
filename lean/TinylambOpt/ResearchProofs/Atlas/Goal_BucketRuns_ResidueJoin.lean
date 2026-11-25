import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E2

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BucketRuns_ResidueJoin

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns

/-- The goal reduces to the generic residue-join lemma: the BucketRuns lanes
cover exactly the physical rows (as a permutation), and for such a cover the
sum of per-row join multiplicities equals the sum of `joinCount` over lanes.
That lemma chain is already established for this model. -/
theorem sound_BucketRuns_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns.ResidueJoin.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E2.BucketRuns_ResidueJoin

end TinylambOpt.ResearchProofs.Atlas.Goal_BucketRuns_ResidueJoin
