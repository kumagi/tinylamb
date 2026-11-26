import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `DashComment.LineRestart` — A newline supplies an unconditional restart point for
  independent workers.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_LineRestart

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : DashComment.LineRestart.Goal := by
  apply Common.lineRestart_of_lfReset DashComment.model
  intro q
  by_cases hq : q = 2 <;> simp [DashComment.model, hq, Common.sym_beq_true, Common.sym_beq_false]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_LineRestart
