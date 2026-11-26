import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `NestedComment.LineRestart` — A newline supplies an unconditional restart point for
  independent workers.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_LineRestart

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ NestedComment.LineRestart.Goal := by
  intro h
  have hh := h [Sym.openComment] [Sym.semi]
  have hneq : (scan NestedComment.model 0 ([Sym.openComment] ++ [Sym.lf] ++ [Sym.semi])).2 ≠
      joinAt ([Sym.openComment].length+1) (scan NestedComment.model 0 ([Sym.openComment]++[Sym.lf])).2 (scan NestedComment.model 0 [Sym.semi]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_LineRestart
