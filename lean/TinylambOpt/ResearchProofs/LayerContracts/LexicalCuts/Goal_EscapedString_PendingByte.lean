import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `EscapedString.PendingByte` — `∀ c, model.step 2 c = (1,false)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_PendingByte

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : EscapedString.PendingByte.Goal := by
  intro c
  simp [EscapedString.model, escaped]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_PendingByte
