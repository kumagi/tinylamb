import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `EscapedString.LineRestart` — A newline supplies an unconditional restart point for
  independent workers.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_LineRestart

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ EscapedString.LineRestart.Goal := by
  intro h
  have hh := h [Sym.sq] [Sym.semi]
  have hneq : (scan EscapedString.model 0 ([Sym.sq] ++ [Sym.lf] ++ [Sym.semi])).2 ≠
      joinAt ([Sym.sq].length+1) (scan EscapedString.model 0 ([Sym.sq]++[Sym.lf])).2 (scan EscapedString.model 0 [Sym.semi]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_LineRestart
