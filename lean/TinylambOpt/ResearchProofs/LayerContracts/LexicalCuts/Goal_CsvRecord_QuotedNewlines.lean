import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `CsvRecord.QuotedNewlines` — `∀ n, scan model 1 (List.replicate n .lf)=(1,[])`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_QuotedNewlines

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : CsvRecord.QuotedNewlines.Goal := by
  have hs : CsvRecord.model.step 1 Sym.lf = (1,false) := by native_decide
  intro n
  induction n with
  | zero => simp [scan]
  | succ n ih =>
      rw [List.replicate_succ, Common.scan_cons, hs]
      simp [ih]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_QuotedNewlines
