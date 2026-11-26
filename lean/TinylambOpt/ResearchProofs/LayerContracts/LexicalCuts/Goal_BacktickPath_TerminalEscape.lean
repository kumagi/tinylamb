import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `BacktickPath.TerminalEscape` — `∀ c, (model.step 2 c).1=1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_TerminalEscape

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : BacktickPath.TerminalEscape.Goal := by
  intro c
  simp [BacktickPath.model, escaped]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_TerminalEscape
