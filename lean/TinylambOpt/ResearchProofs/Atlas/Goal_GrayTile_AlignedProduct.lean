import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.WaveLaneAndGrayTileProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `GrayTile.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout c).map (fun
  r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GrayTile_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile

/-- Exhaustive check over 1,687,500 small inputs
    (`n ≤ 8`, `width ≤ 5`, `base/step/b.base/b.step ∈ [-2,2]`,
    `b.start ≤ 3`, `0 < b.stride ≤ 3`, `b.n ≤ 4`) found no counterexample.
    The proof reuses the established generic row/lane aligned-product lemma
    instantiated for `GrayTile.layout` in `Auto_EncodedPages_P9`. -/
theorem sound_GrayTile_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.WaveLaneAndGrayTileProgressions.GrayTile.GrayTile_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_GrayTile_AlignedProduct
