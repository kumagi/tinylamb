import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `CsvRecord.ColdChunk` — Can each chunk be scanned independently from the neutral
  state?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_ColdChunk

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ CsvRecord.ColdChunk.Goal := by
  intro h
  have hh := h [Sym.dq] [Sym.lf]
  have hneq : (scan CsvRecord.model 0 ([Sym.dq]++[Sym.lf])).2 ≠
      joinAt [Sym.dq].length (scan CsvRecord.model 0 [Sym.dq]).2 (scan CsvRecord.model 0 [Sym.lf]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_ColdChunk
