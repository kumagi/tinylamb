import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `DictionaryDrift.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
-/

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
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions.DictionaryDrift_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryDrift_DeleteProgression
