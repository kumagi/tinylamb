import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `DashComment.ColdChunk` — Can each chunk be scanned independently from the neutral
  state?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_ColdChunk

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ DashComment.ColdChunk.Goal := by
  intro h
  have hh := h [Sym.dash] [Sym.dash, Sym.semi]
  have hneq : (scan DashComment.model 0 ([Sym.dash]++[Sym.dash, Sym.semi])).2 ≠
      joinAt [Sym.dash].length (scan DashComment.model 0 [Sym.dash]).2 (scan DashComment.model 0 [Sym.dash, Sym.semi]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_ColdChunk
