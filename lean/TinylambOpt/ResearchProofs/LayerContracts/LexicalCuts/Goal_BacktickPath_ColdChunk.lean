import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `BacktickPath.ColdChunk` — Can each chunk be scanned independently from the neutral
  state?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_ColdChunk

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ BacktickPath.ColdChunk.Goal := by
  intro h
  have hh := h [Sym.tick] [Sym.dot]
  have hneq : (scan BacktickPath.model 0 ([Sym.tick]++[Sym.dot])).2 ≠
      joinAt [Sym.tick].length (scan BacktickPath.model 0 [Sym.tick]).2 (scan BacktickPath.model 0 [Sym.dot]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_ColdChunk
