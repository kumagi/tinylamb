import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `DictionaryCycle.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryCycle_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle

/-- The dictionary-cycle lanes partition the live physical rows: lane `r`
covers the ordinals congruent to `r` modulo `dict.length`, each carrying the
constant value `dict[r]`.  Hence the live sum is the sum over lanes of
`partialSum a a.n - deletedSum a mask`, which is exactly the generic
lane-wise deletion lemma already established for this layout. -/
theorem sound_DictionaryCycle_DeleteProgression :
    DictionaryCycle.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryAndSignProgressions.DictionaryCycle_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryCycle_DeleteProgression
