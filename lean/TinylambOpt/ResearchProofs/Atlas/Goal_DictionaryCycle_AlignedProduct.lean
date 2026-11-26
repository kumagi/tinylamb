import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `DictionaryCycle.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout
  c).map (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryCycle_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle

/-- Brute-force sanity check (small inputs, `0 < b.stride`) confirms the goal holds.
    The proof reuses the established generic aligned-product lemma instantiated
    for `DictionaryCycle.layout` in `Auto_EncodedPages_E3`. -/
theorem sound_DictionaryCycle_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions.DictionaryCycle_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryCycle_AlignedProduct
