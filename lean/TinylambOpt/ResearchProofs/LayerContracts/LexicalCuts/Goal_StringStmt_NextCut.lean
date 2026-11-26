import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `StringStmt.NextCut` — Return the first statement/field boundary without looking
  beyond it.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_StringStmt_NextCut

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : StringStmt.NextCut.Goal := by
  exact Common.nextCut_general StringStmt.model

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_StringStmt_NextCut
