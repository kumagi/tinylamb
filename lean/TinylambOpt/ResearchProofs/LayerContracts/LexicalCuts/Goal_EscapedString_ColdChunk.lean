import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `EscapedString.ColdChunk` — Can each chunk be scanned independently from the neutral
  state?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_ColdChunk

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ EscapedString.ColdChunk.Goal := by
  intro h
  have hh := h [Sym.sq] [Sym.semi]
  have hneq : (scan EscapedString.model 0 ([Sym.sq]++[Sym.semi])).2 ≠
      joinAt [Sym.sq].length (scan EscapedString.model 0 [Sym.sq]).2 (scan EscapedString.model 0 [Sym.semi]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_ColdChunk
