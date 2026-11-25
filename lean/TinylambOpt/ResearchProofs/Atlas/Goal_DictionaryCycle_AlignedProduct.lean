import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E3

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryCycle_AlignedProduct

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle

/-- Brute-force sanity check (small inputs, `0 < b.stride`) confirms the goal holds.
    The proof reuses the established generic aligned-product lemma instantiated
    for `DictionaryCycle.layout` in `Auto_EncodedPages_E3`. -/
theorem sound_DictionaryCycle_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle.AlignedProduct.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E3.DictionaryCycle_AlignedProduct

end TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryCycle_AlignedProduct
