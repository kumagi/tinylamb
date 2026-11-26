import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_LineRestart

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ ArrayComma.LineRestart.Goal := by
  intro h
  have hh := h [Sym.openArray] [Sym.comma]
  have hneq : (scan ArrayComma.model 0 ([Sym.openArray] ++ [Sym.lf] ++ [Sym.comma])).2 ≠
      joinAt ([Sym.openArray].length+1) (scan ArrayComma.model 0 ([Sym.openArray]++[Sym.lf])).2 (scan ArrayComma.model 0 [Sym.comma]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_LineRestart
