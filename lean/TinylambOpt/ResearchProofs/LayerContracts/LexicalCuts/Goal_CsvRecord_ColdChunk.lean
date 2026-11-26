import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

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
