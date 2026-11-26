import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.OrdinalProgressionsAndBucketRuns

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `SegmentedSlope.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout
  c).map (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SegmentedSlope_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope

/-- The two stride-one lanes of SegmentedSlope decode exactly the physical rows,
    so the generic aligned-product lemma for row/lane covers applies. The proof
    is supplied by the already checked generic development. -/
theorem sound_SegmentedSlope_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.OrdinalProgressionsAndBucketRuns.sound_SegmentedSlope_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_SegmentedSlope_AlignedProduct
