import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `StringStmt.DiscardCutPrefix` — After the first emitted delimiter the suffix can be
  reparsed from zero.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_StringStmt_DiscardCutPrefix

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : StringStmt.DiscardCutPrefix.Goal := by
  apply Common.discard_of_cutResets StringStmt.model
  intro q c h
  cases c <;> simp [StringStmt.model, quoted, Common.sym_beq_true, Common.sym_beq_false] at h ⊢ <;> omega

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_StringStmt_DiscardCutPrefix
