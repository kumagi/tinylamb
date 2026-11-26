import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `QuotedField.LineRestart` — A newline supplies an unconditional restart point for
  independent workers.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_LineRestart

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ QuotedField.LineRestart.Goal := by
  intro h
  have hh := h [Sym.dq] [Sym.dot]
  have hneq : (scan QuotedField.model 0 ([Sym.dq] ++ [Sym.lf] ++ [Sym.dot])).2 ≠
      joinAt ([Sym.dq].length+1) (scan QuotedField.model 0 ([Sym.dq]++[Sym.lf])).2 (scan QuotedField.model 0 [Sym.dot]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_LineRestart
