import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_LineRestart

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ DollarPair.LineRestart.Goal := by
  intro h
  have hh := h [Sym.dollar, Sym.dollar] [Sym.semi]
  have hneq : (scan DollarPair.model 0 ([Sym.dollar, Sym.dollar] ++ [Sym.lf] ++ [Sym.semi])).2 ≠
      joinAt ([Sym.dollar, Sym.dollar].length+1) (scan DollarPair.model 0 ([Sym.dollar, Sym.dollar]++[Sym.lf])).2 (scan DollarPair.model 0 [Sym.semi]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_LineRestart
