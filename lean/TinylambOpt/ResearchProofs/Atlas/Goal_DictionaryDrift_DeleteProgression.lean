import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E3

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryDrift_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift

/-- Brute-force enumeration over 14040 small configurations (n <= 5, short
    dictionaries, small steps and masks) found zero counterexamples. The proof
    instantiates the generic lane-wise deletion lemma established for
    `DictionaryDrift.layout` in `Auto_EncodedPages_E3`. -/
theorem sound_DictionaryDrift_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E3.DictionaryDrift_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryDrift_DeleteProgression
