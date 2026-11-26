import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `CsvRecord.SkipPlain` — Bypass scanning bytes with no control symbols, even while
  resuming a partial token.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_SkipPlain

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : CsvRecord.SkipPlain.Goal := by
  apply Common.skipPlain_of_stable CsvRecord.model
  intro q c hc
  cases c <;> simp [CsvRecord.model, quoted, Common.sym_beq_true, Common.sym_beq_false] at hc ⊢

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_SkipPlain
