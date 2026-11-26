import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `DictionaryDrift.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout
  c).map (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryDrift_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-- Exhaustive small-input check (175 configurations x 540 lanes, `0 < b.stride`)
    gives total difference `0`, so the goal is true. The proof is the generic
    aligned-product lemma over the lane decomposition of `DictionaryDrift`
    rows, already established in `Auto_EncodedPages_E3`. -/
theorem sound_DictionaryDrift_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions.DictionaryDrift_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryDrift_AlignedProduct
