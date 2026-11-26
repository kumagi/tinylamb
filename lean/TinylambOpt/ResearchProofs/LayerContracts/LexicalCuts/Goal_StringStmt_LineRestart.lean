import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_StringStmt_LineRestart

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ StringStmt.LineRestart.Goal := by
  intro h
  have hh := h [Sym.sq] [Sym.semi]
  have hneq : (scan StringStmt.model 0 ([Sym.sq] ++ [Sym.lf] ++ [Sym.semi])).2 ≠
      joinAt ([Sym.sq].length+1) (scan StringStmt.model 0 ([Sym.sq]++[Sym.lf])).2 (scan StringStmt.model 0 [Sym.semi]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_StringStmt_LineRestart
