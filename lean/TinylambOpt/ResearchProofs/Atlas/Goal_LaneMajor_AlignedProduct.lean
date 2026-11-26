import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.WaveAndLaneProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `LaneMajor.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout c).map
  (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

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
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.WaveAndLaneProgressions.LaneMajor.LaneMajor_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_LaneMajor_AlignedProduct
