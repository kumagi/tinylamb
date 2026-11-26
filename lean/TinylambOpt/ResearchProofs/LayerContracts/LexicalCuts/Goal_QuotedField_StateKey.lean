import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `QuotedField.StateKey` — Can a compiled chunk summary be indexed by only the proposed
  entry-state class?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_StateKey

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : QuotedField.StateKey.Goal := by
  exact Common.stateKey_of_injective QuotedField.model (by intro a b h; exact h)

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_StateKey
