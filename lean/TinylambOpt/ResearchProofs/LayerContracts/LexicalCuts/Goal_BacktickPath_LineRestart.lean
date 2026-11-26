import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `BacktickPath.LineRestart` — A newline supplies an unconditional restart point for
  independent workers.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_LineRestart

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ BacktickPath.LineRestart.Goal := by
  intro h
  have hh := h [Sym.tick] [Sym.dot]
  have hneq : (scan BacktickPath.model 0 ([Sym.tick] ++ [Sym.lf] ++ [Sym.dot])).2 ≠
      joinAt ([Sym.tick].length+1) (scan BacktickPath.model 0 ([Sym.tick]++[Sym.lf])).2 (scan BacktickPath.model 0 [Sym.dot]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_LineRestart
