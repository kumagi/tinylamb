import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.TailRunAndWrappingResidueJoins

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `BucketRuns.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
-/

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
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.TailRunAndWrappingResidueJoins.BucketRuns_ResidueJoin

end TinylambOpt.ResearchProofs.Atlas.Goal_BucketRuns_ResidueJoin
