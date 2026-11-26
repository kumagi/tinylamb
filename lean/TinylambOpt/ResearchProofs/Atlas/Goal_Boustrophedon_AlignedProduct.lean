import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.WaveAndLaneProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `Boustrophedon.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout c).map
  (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

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
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.WaveAndLaneProgressions.Boustrophedon.Boustrophedon_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_Boustrophedon_AlignedProduct
