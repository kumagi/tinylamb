import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_ColdChunk

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ QuotedField.ColdChunk.Goal := by
  intro h
  have hh := h [Sym.dq] [Sym.dot]
  have hneq : (scan QuotedField.model 0 ([Sym.dq]++[Sym.dot])).2 ≠
      joinAt [Sym.dq].length (scan QuotedField.model 0 [Sym.dq]).2 (scan QuotedField.model 0 [Sym.dot]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_ColdChunk
