import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `DollarPair.ColdChunk` — Can each chunk be scanned independently from the neutral
  state?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_ColdChunk

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ DollarPair.ColdChunk.Goal := by
  intro h
  have hh := h [Sym.dollar, Sym.dollar] [Sym.semi]
  have hneq : (scan DollarPair.model 0 ([Sym.dollar, Sym.dollar]++[Sym.semi])).2 ≠
      joinAt [Sym.dollar, Sym.dollar].length (scan DollarPair.model 0 [Sym.dollar, Sym.dollar]).2 (scan DollarPair.model 0 [Sym.semi]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_ColdChunk
