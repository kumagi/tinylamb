import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `AlternatingSign.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout
  c).map (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingSign_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign

/-- Brute-force check over 343 small configurations and 9604 lanes (all
    `0 < b.stride`, `n ≤ 6`, values in `[-3,3]`) found no counterexample.
    The proof reuses the established generic row/lane aligned-product lemma
    instantiated for `AlternatingSign.layout` in `Auto_EncodedPages_E3`. -/
theorem sound_AlternatingSign_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions.AlternatingSign_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingSign_AlignedProduct
