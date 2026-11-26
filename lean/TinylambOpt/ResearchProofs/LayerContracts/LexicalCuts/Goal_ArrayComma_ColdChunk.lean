import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `ArrayComma.ColdChunk` — Can each chunk be scanned independently from the neutral
  state?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_ColdChunk

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ ArrayComma.ColdChunk.Goal := by
  intro h
  have hh := h [Sym.openArray] [Sym.comma]
  have hneq : (scan ArrayComma.model 0 ([Sym.openArray]++[Sym.comma])).2 ≠
      joinAt [Sym.openArray].length (scan ArrayComma.model 0 [Sym.openArray]).2 (scan ArrayComma.model 0 [Sym.comma]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_ColdChunk
